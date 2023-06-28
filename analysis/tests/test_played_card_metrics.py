# -*- coding: utf-8 -*-
import os
import numpy as np
from utils import turn_metric_helpers
import pandas as pd
import pytest


class TestPlayedCardMetrics:
    turns = [
        {
            "game": 2,
            "turn": 1,
            "values": {
                "strict_switch_count": 0,
                "ambiguous_switch_count": 0,
                "strict_repeat_count": 0,
                "num_of_cards_played_in_turn": 0,
                "x_variable_switches": 0,
                "num_cards_in_hand_at_start_of_turn": 7,
                "num_play_not_allowed": 0,
                "max_possible_cards_playable": 7,
                "time_to_first_action": 45.45800018310547,
                "first_match_in_turn": None,
            },
        },
        {
            "game": 4,
            "turn": 1,
            "values": {
                "strict_switch_count": 2,
                "ambiguous_switch_count": 0,
                "strict_repeat_count": 1,
                "num_of_cards_played_in_turn": 5,
                "x_variable_switches": 1,
                "num_cards_in_hand_at_start_of_turn": 7,
                "num_play_not_allowed": 0,
                "max_possible_cards_playable": 5,
                "time_to_first_action": 134.39999985694885,
                "first_match_in_turn": "value_algebraic",
            },
        },
        {
            "game": 12,
            "turn": 0,
            "values": {
                "strict_switch_count": 2,
                "ambiguous_switch_count": 2,
                "strict_repeat_count": 1,
                "num_of_cards_played_in_turn": 7,
                "x_variable_switches": 2,
                "num_cards_in_hand_at_start_of_turn": 7,
                "num_play_not_allowed": 0,
                # missing events to calculate these
                # "time_to_first_action": None,
                # "max_possible_cards_playable": None,
                "first_match_in_turn": "color",
            },
        },
    ]
    dir_path = os.path.dirname(os.path.realpath(__file__))
    example_df = pd.read_csv(os.path.join(dir_path, "../tests/data/abc.csv"))
    output_df = turn_metric_helpers.calc_turn_metrics(example_df)

    @pytest.mark.parametrize("turn", turns)
    def test_switch_from_df(self, turn):
        turn_df = self.output_df[
            (self.output_df.user_game_index == turn["game"])
            & (self.output_df.user_turn_start_index == turn["turn"])
        ]
        for key, value in turn["values"].items():
            assert turn_df[key].values[0] == value, f"{turn['game']}:{turn['turn']} - {key},{value}"
