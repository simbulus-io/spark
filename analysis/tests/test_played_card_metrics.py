import json
from utils import turn_metric_helpers
import pandas as pd
import pytest


class TestPlayedCardMetrics:
    # setup dataset:
    # match_columns = [f'match_{val}' for val in ['color', 'value', 'algebraic']]
    # columns= ['user_id', 'user_game_index', 'user_turn_start_index', 'event_name', 'user_action_time', 'user_turn_start_time'] + match_columns
    # temp_df = equivacards_events[(equivacards_events.user_id =='4491') &
    #                             (equivacards_events.event_name=='user_played_card')][columns]
    # temp_df.user_id = 'abc'
    # temp_df.to_csv('analysis/tests/data/abc.csv')

    turns = [
        {
            "game": 2,
            "turn": 1,
            "cards_played": 0,
            "strict_switch": 0,
            "ambiguous_switch": 0,
            "strict_repeat": 0,
            "x_variable_switch": 0,
        },
        {
            "game": 4,
            "turn": 1,
            "cards_played": 5,
            "strict_switch": 2,
            "ambiguous_switch": 0,
            "strict_repeat": 1,
            "x_variable_switch": 0,
        },
        {
            "game": 12,
            "turn": 0,
            "cards_played": 7,
            "strict_switch": 1,
            "ambiguous_switch": 2,
            "strict_repeat": 1,
            "x_variable_switch": 2,
        },
    ]

    matches = [
        {"current": [1, 0, 0], "previous": [1, 0, 0], "label": "strict_repeat"},
        {"current": [1, 0, 0], "previous": [0, 1, 0], "label": "strict_switch"},
        {"current": [1, 0, 0], "previous": [0, 0, 1], "label": "strict_switch"},
        {"current": [1, 0, 0], "previous": [0, 1, 1], "label": "strict_switch"},
        {"current": [1, 0, 0], "previous": [0, 0, 0], "label": "other"},
        {"current": [0, 0, 0], "previous": [1, 0, 0], "label": "white_card"},
        {"current": [1, 0, 0], "previous": [1, 1, 0], "label": "ambiguous_switch"},
        {"current": [1, 0, 0], "previous": [1, 0, 1], "label": "ambiguous_switch"},
        {"current": [1, 1, 0], "previous": [0, 1, 0], "label": "ambiguous_switch"},
        {"current": [1, 1, 0], "previous": [1, 1, 0], "label": "ambiguous_switch"},
        {"current": [1, 1, 0], "previous": [1, 1, 0], "label": "ambiguous_switch"},
        {"current": [1, 0, 0], "previous": [1, 1, 1], "label": "ambiguous_switch"},
        {"current": [1, 1, 1], "previous": [1, 1, 1], "label": "ambiguous_switch"},
    ]

    example_df = pd.read_csv("./analysis/tests/data/abc.csv")

    output_df = turn_metric_helpers.cal_switches(example_df)
    assert output_df.shape[0] == 90

    @pytest.mark.parametrize("match", matches)
    def test_switches(self, match):
        assert (
            turn_metric_helpers.calc_switch_type(match["current"], match["previous"])
            == match["label"]
        )

    @pytest.mark.parametrize("turn", turns)
    def test_switch_from_df(self, turn):
        turn_df = self.output_df[
            (self.output_df.user_game_index == turn["game"])
            & (self.output_df.user_turn_start_index == turn["turn"])
        ]
        assert (
            turn_df[turn_df.switch_type == "strict_repeat"].shape[0]
            == turn["strict_repeat"]
        )

        assert (
            turn_df[turn_df.switch_type == "strict_switch"].shape[0]
            == turn["strict_switch"]
        )
        assert (
            turn_df[turn_df.switch_type == "ambiguous_switch"].shape[0]
            == turn["ambiguous_switch"]
        )
