# -*- coding: utf-8 -*-
from datetime import datetime
import numpy as np
import pandas as pd
import json
from bson import json_util


def get_from_db():
    import saga_py_aws
    import saga_py_mongo

    secret = saga_py_aws.get_mongo_uri_secret("prod")
    client = saga_py_mongo.get_mongo_client(secret)

    mapping_df = pd.read_csv("./data/cognition_mapping.csv", dtype={"user_id": str})

    query = [
        {"$match": {"activity": "EQUIVACARDS"}},
        {"$match": {"user_id": {"$in": list(mapping_df.user_id.values)}}},
    ]

    results = saga_py_mongo.aggregate_query_mongo(
        aggregate_query=query,
        collection_client=client["xlr8_beta"]["analytics_simple_event_data"],
    )
    with open("./data/analysis_set.json", "w+") as f:
        json.dump(json.loads(json_util.dumps(results)), f)


def unnest_dict(event):
    event_copy = event.copy()
    if type(event) == dict:
        keys = event.keys()
        for key in keys:
            if type(event_copy[key]) == dict:
                event_copy.update(unnest_dict(event_copy[key]))
                del event_copy[key]
    return event_copy


def align_unix_convention(x):
    if not np.isnan(x.server_timestamp):
        return x.server_timestamp / 1000
    else:
        ts = x.timestamp
        above_12 = np.floor(np.log10(ts) - 9)
        return ts / (10 ** (above_12))


def align_timestamps(input_df):
    output_df = input_df.copy()

    output_df["unix_timestamp_combined"] = output_df.apply(lambda x: align_unix_convention(x), axis=1)

    output_df["timestamp"] = output_df.apply(
        lambda x: datetime.utcfromtimestamp(x.unix_timestamp_combined), axis=1
    )
    return output_df


def correct_launch_activity(x):
    if x == "launched_connect_the_drops":
        return "CONNECT_THE_DROPS"
    elif x == "launched_equivacards":
        return "EQUIVACARDS"


def add_game_turn_boundary_flags(input_df):
    equiv_events_df = input_df.sort_values(by="unix_timestamp_combined").copy()

    equiv_events_df["game_launch"] = equiv_events_df.event_name.apply(
        lambda x: x in ["launched_connect_the_drops", "launched_equivacards"]
    )
    equiv_events_df["game_start"] = equiv_events_df.event_name.apply(
        lambda x: x in ["launched_equivacards", "initial_game_state", "play_again_yes"]
    )
    equiv_events_df["game_start_time"] = equiv_events_df.apply(
        lambda x: x.unix_timestamp_combined if x.game_start else np.nan, axis=1
    )

    equiv_events_df["corrected_activity"] = equiv_events_df.event_name.apply(correct_launch_activity)
    equiv_events_df["game_end"] = equiv_events_df.event_name.apply(lambda x: x in ["user_won", "user_lost"])
    equiv_events_df["game_end_time"] = equiv_events_df.apply(
        lambda x: x.unix_timestamp_combined if x.game_end else np.nan, axis=1
    )
    equiv_events_df["user_turn_start"] = equiv_events_df.event_name.apply(
        lambda x: x in ["user_turn", "deal_ended"]
    )
    equiv_events_df["user_turn_start_time"] = equiv_events_df.apply(
        lambda x: x.unix_timestamp_combined if x.user_turn_start else np.nan, axis=1
    )
    equiv_events_df["user_turn_end"] = equiv_events_df.event_name.apply(
        lambda x: x in ["user_drew_card", "user_won", "user_lost"]
    )
    equiv_events_df["user_turn_end_time"] = equiv_events_df.apply(
        lambda x: x.unix_timestamp_combined if x.user_turn_end else np.nan, axis=1
    )
    equiv_events_df["user_took_action"] = equiv_events_df.event_name.apply(
        lambda x: x in ["user_played_card", "user_drew_card", "play_not_allowed"]
    )
    equiv_events_df["user_action_time"] = equiv_events_df.apply(
        lambda x: x.unix_timestamp_combined if x.user_took_action else np.nan, axis=1
    )
    equiv_events_df["user_launch_index"] = equiv_events_df.groupby("user_id").game_launch.cumsum()
    equiv_events_df["user_game_index"] = equiv_events_df.groupby("user_id").game_start.cumsum()
    equiv_events_df["user_turn_start_index"] = equiv_events_df.groupby(
        ["user_id", "user_game_index"]
    ).user_turn_start.cumsum()
    equiv_events_df["user_turn_end_index"] = equiv_events_df.groupby(
        ["user_id", "user_game_index"]
    ).user_turn_end.cumsum()

    equiv_events_df["turn_id"] = equiv_events_df.apply(
        lambda x: "-".join([str(x.user_turn_start_index), str(x.user_turn_end_index)]),
        axis=1,
    )
    equiv_events_df["comp_turn"] = equiv_events_df.apply(
        lambda x: x.user_turn_start_index == x.user_turn_end_index, axis=1
    )
    equiv_events_df["user_turn"] = ~equiv_events_df["comp_turn"]

    equiv_events_df["p1_hand_size_change"] = equiv_events_df.event_name.apply(
        lambda x: -1 if x == "user_played_card" else 1 if x == "user_drew_card" else None
    )

    equiv_events_df["p1_hand_size"] = equiv_events_df.apply(
        lambda x: len(x.p1_hand) if type(x.p1_hand) == list else None, axis=1
    )
    equiv_events_df["next_p1_hand_size"] = equiv_events_df.groupby(
        ["user_id", "user_game_index", "user_turn_start_index"]
    ).p1_hand_size.shift(-1)
    equiv_events_df["p1_hand_size_pre_action"] = (
        equiv_events_df.next_p1_hand_size - equiv_events_df.p1_hand_size_change
    )

    game_by_launch_df = equiv_events_df[equiv_events_df["corrected_activity"].notna()][
        ["user_id", "user_launch_index", "corrected_activity"]
    ]
    corrected_activity_events_df = pd.merge(
        equiv_events_df.drop("corrected_activity", axis=1),
        game_by_launch_df,
        on=["user_id", "user_launch_index"],
    )
    return corrected_activity_events_df.drop(["activity", "p1_hand_size_temp"], axis=1, errors="ignore")
