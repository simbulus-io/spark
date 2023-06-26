# -*- coding: utf-8 -*-
import pandas as pd

match_types = ["color", "value", "algebraic"]


def calc_turn_metrics(input_df):
    event_df = input_df.copy()
    # time_to_first_move_seconds
    time_to_first_move_df = event_df.groupby(
        ["user_id", "user_game_index", "user_turn_start_index"]
    ).apply(
        lambda x: (
            x.user_action_time.min() - x.user_turn_start_time.min()
        ).total_seconds()
    )

    # played card based metrics

    # num_of_cards_played_in_turn
    user_played_card_df = event_df[event_df.event_name == "user_played_card"].copy()
    num_of_cards_played__df = (
        user_played_card_df.groupby(
            ["user_id", "user_game_index", "user_turn_start_index"]
        )
        .card.count()
        .reset_index()
    )

    return pd.merge(time_to_first_move_df, num_of_cards_played__df)


def cal_switches(input_df):
    event_df = input_df.copy()
    user_played_card_df = event_df[event_df.event_name == "user_played_card"].copy()
    match_types = ["color", "value", "algebraic"]
    for match_type in match_types:
        user_played_card_df[f"previous_match_{match_type}"] = user_played_card_df[
            f"match_{match_type}"
        ].shift(1)
        user_played_card_df[f"previous_match_{match_type}"] == user_played_card_df[
            f"match_{match_type}"
        ]
    match_columns = [f"match_{match_type}" for match_type in match_types]
    previous_columns = [f"previous_match_{match_type}" for match_type in match_types]
    user_played_card_df["switch_type"] = user_played_card_df.apply(
        lambda x: calc_switch_type(x[match_columns].values, x[previous_columns].values),
        axis=1,
    )
    return user_played_card_df


def calc_switch_type(current_matches, previous_matches):
    if type(current_matches[0]) == float:
        return "other"
    if sum(current_matches) == 0:
        return "white_card"
    if (sum(current_matches) == 1) and (
        all([str(a) == str(b) for a, b in zip(current_matches, previous_matches)])
    ):
        return "strict_repeat"
    elif (
        (sum(current_matches) == 1)
        and (sum(previous_matches) >= 1)
        and (
            all(
                [
                    str(a) != str(b)
                    for a, b in zip(current_matches, previous_matches)
                    if a == 1
                ]
            )
        )
    ):
        return "strict_switch"
    elif (
        (sum(current_matches) >= 1)
        and (sum(previous_matches) >= 1)
        and (
            sum([str(a) == str(b) for a, b in zip(current_matches, previous_matches)])
            >= 1
        )
    ):
        return "ambiguous_switch"
    else:
        return "other"


# first_match_in_turn

# x_variable_switches

# num_cards_in_hand_at_start_of_turn

# max_possible_cards_playable

# num_play_not_allowed

# total_turn_time
