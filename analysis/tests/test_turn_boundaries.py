# -*- coding: utf-8 -*-
from utils import clean_up_helpers
import pandas as pd
import pytest


class TestTurnBoundaries:
    # fmt: off
    users = [
        {"id": "123", "expected_games": 1, "expected_turns": 1},
        {"id": "124", "expected_games": 1, "expected_turns": 3},
        {"id": "125", "expected_games": 3, "expected_turns": 2},
    ]

    events = [
        # 123
        {"event_name": "launched_equivacards", "user_id": "123", "unix_timestamp_combined": 1, "p1_hand": []},
        {"event_name": "user_turn", "user_id": "123", "unix_timestamp_combined": 2},
        {"event_name": "user_played_card", "user_id": "123", "unix_timestamp_combined": 3},
        {"event_name": "user_played_card", "user_id": "123", "unix_timestamp_combined": 3},
        {"event_name": "user_played_card", "user_id": "123", "unix_timestamp_combined": 3},
        {"event_name": "user_drew_card", "user_id": "123", "unix_timestamp_combined": 4},
        # 124
        {"event_name": "launched_equivacards", "user_id": "124", "unix_timestamp_combined": 1},
        {"event_name": "user_turn", "user_id": "124", "unix_timestamp_combined": 2},
        {"event_name": "user_drew_card", "user_id": "124", "unix_timestamp_combined": 3},
        {"event_name": "user_turn", "user_id": "124", "unix_timestamp_combined": 4},
        {"event_name": "user_drew_card", "user_id": "124", "unix_timestamp_combined": 5},
        {"event_name": "user_turn", "user_id": "124", "unix_timestamp_combined": 6},
        {"event_name": "user_drew_card", "user_id": "124", "unix_timestamp_combined": 7},
        # 125
        {"event_name": "launched_equivacards", "user_id": "125", "unix_timestamp_combined": 1},
        {"event_name": "user_turn", "user_id": "125", "unix_timestamp_combined": 2},
        {"event_name": "launched_equivacards", "user_id": "125", "unix_timestamp_combined": 3},
        {"event_name": "user_turn", "user_id": "125", "unix_timestamp_combined": 4},
        {"event_name": "user_drew_card", "user_id": "125", "unix_timestamp_combined": 5},
        {"event_name": "user_turn", "user_id": "125", "unix_timestamp_combined": 6},
        {"event_name": "user_lost", "user_id": "125", "unix_timestamp_combined": 7},
        {"event_name": "play_again_yes", "user_id": "125", "unix_timestamp_combined": 8},
        {"event_name": "user_turn", "user_id": "125", "unix_timestamp_combined": 9},
        {"event_name": "user_drew_card", "user_id": "125", "unix_timestamp_combined": 10},
        {"event_name": "user_turn", "user_id": "125", "unix_timestamp_combined": 11},
        {"event_name": "user_drew_card", "user_id": "125", "unix_timestamp_combined": 12},
    ]
    # fmt: on

    events_df = pd.DataFrame(events)
    output_df = clean_up_helpers.add_game_turn_boundary_flags(events_df)

    @pytest.mark.parametrize("user", users)
    def test_turn_counts(self, user):
        user_df = self.output_df[self.output_df.user_id == user["id"]]
        assert user_df.user_game_index.max() == user["expected_games"]
        assert user_df.user_turn_start_index.max() == user["expected_turns"]
