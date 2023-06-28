# -*- coding: utf-8 -*-
from utils import turn_metric_helpers
import pandas as pd
import pytest


class TestSwitchMetrics:
    matches = [
        {
            "current": [1, 0, 0],
            "previous": [1, 0, 0],
            "label": "strict_repeat",
            "card": "",
            "match_type": "color",
        },
        {
            "current": [1, 0, 0],
            "previous": [0, 1, 0],
            "label": "strict_switch",
            "match_type": "color",
        },
        {
            "current": [1, 0, 0],
            "previous": [0, 0, 1],
            "label": "strict_switch",
            "match_type": "color",
        },
        {
            "current": [1, 0, 0],
            "previous": [0, 1, 1],
            "label": "strict_switch",
            "match_type": "color",
        },
        {
            "current": [1, 0, 0],
            "previous": [0, 0, 0],
            "label": "other",
            "match_type": "color",
        },
        {
            "current": [0, 0, 0],
            "previous": [1, 0, 0],
            "label": "white_card",
            "match_type": " ",
        },
        {
            "current": [1, 0, 0],
            "previous": [1, 1, 0],
            "label": "ambiguous_switch",
            "match_type": "color",
        },
        {
            "current": [1, 0, 0],
            "previous": [1, 0, 1],
            "label": "ambiguous_switch",
            "match_type": "color",
        },
        {
            "current": [1, 1, 0],
            "previous": [0, 1, 0],
            "label": "ambiguous_switch",
            "match_type": "color_value",
        },
        {
            "current": [1, 1, 0],
            "previous": [1, 1, 0],
            "label": "ambiguous_switch",
            "match_type": "color_value",
        },
        {
            "current": [1, 1, 0],
            "previous": [1, 1, 0],
            "label": "ambiguous_switch",
            "match_type": "color_value",
        },
        {
            "current": [1, 0, 0],
            "previous": [1, 1, 1],
            "label": "ambiguous_switch",
            "match_type": "color",
        },
        {
            "current": [1, 1, 1],
            "previous": [1, 1, 1],
            "label": "ambiguous_switch",
            "match_type": "color_value_algebraic",
        },
    ]

    example_df = pd.read_csv("./analysis/tests/data/abc.csv")
    output_df = turn_metric_helpers.calc_switches(example_df)

    @pytest.mark.parametrize("match", matches)
    def test_switch_logic(self, match):
        assert turn_metric_helpers.calc_switch_type(match["current"], match["previous"]) == match["label"]

    # @pytest.mark.parametrize("turn", turns)
    # def test_switch_from_df(self, turn):
    #     turn_df = self.output_df[
    #         (self.output_df.user_game_index == turn["game"])
    #         & (self.output_df.user_turn_start_index == turn["turn"])
    #     ]
    #     count_columns = [
    #         "strict_switch_count",
    #         "ambiguous_switch_count",
    #         "strict_repeat_count",
    #     ]
    #     for count_column in count_columns:
    #         assert (
    #             turn_metric_helpers.calc_turn_metrics(turn_df)[count_column].max()
    #             == turn[count_column]
    #         ), f"{turn['game']} {turn['turn']} {count_column}"
