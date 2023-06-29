# -*- coding: utf-8 -*-
import os
import pandas as pd
import json
from utils import clean_up_helpers, turn_metric_helpers

dir_path = os.path.dirname(os.path.realpath(__file__))
input_filepath = os.path.join(dir_path, "input_data/analysis_set.json")
output_dir = os.path.join(dir_path, "output_data")

with open(input_filepath) as f:
    event_json = json.load(f)


flat_events = []
for event in event_json:
    flat_events.append(clean_up_helpers.unnest_dict(event))

drop_columns = [
    "product",
    "browser_session_id",
    "bucket",
    "whose_turn",
    "game_name",
]

json_df = pd.json_normalize(flat_events, meta=["_id.$oid"]).drop(drop_columns, axis=1, errors="ignore")

aligned_df = clean_up_helpers.align_timestamps(json_df).drop(["server_timestamp"], axis=1, errors="ignore")

candidate_equiv_events_df = aligned_df[aligned_df.activity.isin(["EQUIVACARDS"])]


enriched_events_df = clean_up_helpers.add_game_turn_boundary_flags(candidate_equiv_events_df)


equivacards_events_df = enriched_events_df[enriched_events_df.corrected_activity == "EQUIVACARDS"]


mapping_df = pd.read_csv(os.path.join(dir_path, "input_data/cognition_mapping.csv"), dtype={"user_id": str})
prepped_df = pd.merge(equivacards_events_df, mapping_df)

turn_metrics_df = turn_metric_helpers.calc_turn_metrics(prepped_df)

os.makedirs("./output_data", exist_ok=True)
make_subset = True
subset_user_id = "4491"
if make_subset:
    prepped_df[prepped_df.user_id == subset_user_id].to_csv("./output_data/current.csv")
    match_columns = [f"match_{val}" for val in ["color", "value", "algebraic"]]
    columns = [
        "email",
        "user_id",
        "user_game_index",
        "user_turn_start_index",
        "event_name",
        "user_action_time",
        "user_turn_start_time",
        "user_turn_end_time",
        "card",
        "best_play_length",
        "changed_x_val",
        "p1_hand_size_pre_action",
        "unix_timestamp_combined",
        "timestamp",
    ] + match_columns
    temp_df = prepped_df[prepped_df.user_id == subset_user_id][columns]
    temp_df.user_id = "abc"
    temp_df.to_csv(os.path.join(output_dir, "abc.csv"))
    prepped_df[prepped_df.user_id == subset_user_id].to_csv(
        os.path.join(output_dir, f"{subset_user_id}_enriched_events.csv")
    )

    turn_metrics_df[turn_metrics_df.user_id == subset_user_id].to_csv(
        os.path.join(output_dir, f"{subset_user_id}_turn_level_metrics.csv")
    )


prepped_df[
    [
        "timestamp",
        "email",
        "user_id",
        "event_name",
        "$oid",
        "board",
        "p1_hand",
        "p2_ncards",
        "p1_hand_size_change",
        "p1_hand_size",
        "next_p1_hand_size",
        "p1_hand_size_pre_action",
        "card",
        "value",
        "best_play_length",
        "best_play",
        "match_color",
        "match_value",
        "match_algebraic",
        "n_cards_played",
        "changed_x_val",
        "changed_pile_val",
        "game_length",
        "deck_version",
        "unix_timestamp_combined",
        "game_launch",
        "game_start",
        "game_start_time",
        "game_end",
        "game_end_time",
        "user_turn_start",
        "user_turn_start_time",
        "user_turn_end",
        "user_turn_end_time",
        "user_took_action",
        "user_action_time",
        "user_launch_index",
        "user_game_index",
        "user_turn_start_index",
        "user_turn_end_index",
        "turn_id",
        "comp_turn",
        "user_turn",
        "corrected_activity",
    ]
].to_csv(os.path.join(output_dir, "enriched_events.csv"))

turn_metrics_df.to_csv(os.path.join(output_dir, "turn_level_metrics.csv"))
