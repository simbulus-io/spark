import os
import pandas as pd
import json
from utils import clean_up_helpers, turn_metric_helpers


input_filepath = "./input_data/analysis_set.json"


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

json_df = pd.json_normalize(flat_events, meta=["_id.$oid"]).drop(
    drop_columns, axis=1, errors="ignore"
)

aligned_df = clean_up_helpers.align_timestamps(json_df).drop(
    ["server_timestamp"], axis=1, errors="ignore"
)

candidate_equiv_events_df = aligned_df[aligned_df.activity.isin(["EQUIVACARDS"])]


enriched_events_df = clean_up_helpers.add_game_turn_boundary_flags(
    candidate_equiv_events_df
)


equivacards_events_df = enriched_events_df[
    enriched_events_df.corrected_activity == "EQUIVACARDS"
]

turn_metrics = turn_metric_helpers.calc_turn_metrics(equivacards_events_df)


os.makedirs("./output_data", exist_ok=True)
enriched_events_df.to_csv("./output_data/current.csv")
