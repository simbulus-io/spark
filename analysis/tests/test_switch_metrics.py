# -*- coding: utf-8 -*-
import os
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
    dir_path = os.path.dirname(os.path.realpath(__file__))
    example_df = pd.read_csv(os.path.join(dir_path, "../tests/data/abc.csv"))
    output_df = turn_metric_helpers.calc_switches(example_df)

    @pytest.mark.parametrize("match", matches)
    def test_switch_logic(self, match):
        assert turn_metric_helpers.calc_switch_type(match["current"], match["previous"]) == match["label"]
