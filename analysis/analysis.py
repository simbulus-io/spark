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

turn_metrics_df = turn_metric_helpers.calc_turn_metrics(equivacards_events_df)


os.makedirs("./output_data", exist_ok=True)
make_subset = True
subset_user_id = "4491"
if make_subset:
    enriched_events_df[enriched_events_df.user_id == subset_user_id].to_csv("./output_data/current.csv")
    match_columns = [f"match_{val}" for val in ["color", "value", "algebraic"]]
    columns = [
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
    ] + match_columns
    temp_df = equivacards_events_df[equivacards_events_df.user_id == subset_user_id][columns]
    temp_df.user_id = "abc"
    temp_df.to_csv(os.path.join(output_dir, "abc.csv"))
    enriched_events_df[enriched_events_df.user_id == subset_user_id].to_csv(
        os.path.join(output_dir, f"{subset_user_id}_enriched_events.csv")
    )

    turn_metrics_df[turn_metrics_df.user_id == subset_user_id].to_csv(
        os.path.join(output_dir, f"{subset_user_id}_turn_level_metrics.csv")
    )


enriched_events_df.to_csv(os.path.join(output_dir, "enriched_events.csv"))

turn_metrics_df.to_csv(os.path.join(output_dir, "turn_level_metrics.csv"))
