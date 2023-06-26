from utils import clean_up_helpers
import pandas as pd
import pytest


class TestTurnBoundaries:
    users = [
        {"id": "123", "expected_games": 1, "expected_turns": 1},
        {"id": "124", "expected_games": 1, "expected_turns": 3},
        {"id": "125", "expected_games": 3, "expected_turns": 2},
    ]

    events = [
        # 123
        {"event_name": "launched_equivacards", "user_id": "123", "timestamp": 1},
        {"event_name": "user_turn", "user_id": "123", "timestamp": 2},
        {"event_name": "user_played_card", "user_id": "123", "timestamp": 3},
        {"event_name": "user_played_card", "user_id": "123", "timestamp": 3},
        {"event_name": "user_played_card", "user_id": "123", "timestamp": 3},
        {"event_name": "user_drew_card", "user_id": "123", "timestamp": 4},
        # 124
        {"event_name": "launched_equivacards", "user_id": "124", "timestamp": 1},
        {"event_name": "user_turn", "user_id": "124", "timestamp": 2},
        {"event_name": "user_drew_card", "user_id": "124", "timestamp": 3},
        {"event_name": "user_turn", "user_id": "124", "timestamp": 4},
        {"event_name": "user_drew_card", "user_id": "124", "timestamp": 5},
        {"event_name": "user_turn", "user_id": "124", "timestamp": 6},
        {"event_name": "user_drew_card", "user_id": "124", "timestamp": 7},
        # 125
        {"event_name": "launched_equivacards", "user_id": "125", "timestamp": 1},
        {"event_name": "user_turn", "user_id": "125", "timestamp": 2},
        {"event_name": "launched_equivacards", "user_id": "125", "timestamp": 3},
        {"event_name": "user_turn", "user_id": "125", "timestamp": 4},
        {"event_name": "user_drew_card", "user_id": "125", "timestamp": 5},
        {"event_name": "user_turn", "user_id": "125", "timestamp": 6},
        {"event_name": "user_lost", "user_id": "125", "timestamp": 7},
        {"event_name": "play_again_yes", "user_id": "125", "timestamp": 8},
        {"event_name": "user_turn", "user_id": "125", "timestamp": 9},
        {"event_name": "user_drew_card", "user_id": "125", "timestamp": 10},
        {"event_name": "user_turn", "user_id": "125", "timestamp": 11},
        {"event_name": "user_drew_card", "user_id": "125", "timestamp": 12},
    ]

    events_df = pd.DataFrame(events)
    output_df = clean_up_helpers.add_game_turn_boundary_flags(events_df)

    @pytest.mark.parametrize("user", users)
    def test_turn_counts(self, user):
        user_df = self.output_df[self.output_df.user_id == user["id"]]
        assert user_df.user_game_index.max() == user["expected_games"]
        assert user_df.user_turn_start_index.max() == user["expected_turns"]
