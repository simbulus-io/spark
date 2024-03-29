# -*- coding: utf-8 -*-
import pandas as pd

match_types = ["color", "value", "algebraic"]


def calc_turn_metrics(input_df):
    event_df = input_df.copy()
    event_df_w_switches_df = calc_switches(event_df)
    agg_df = (
        event_df_w_switches_df.groupby(
            ["email", "user_id", "user_game_index", "user_turn_start_index"],
            dropna=False,
        ).agg(
            turn_start_date_et=pd.NamedAgg(column="date_tz_et", aggfunc=min),
            turn_start_time_et=pd.NamedAgg(column="timestamp_tz_et", aggfunc=min),
            turn_start=pd.NamedAgg(column="timestamp", aggfunc=min),
            strict_repeat_count=pd.NamedAgg(
                column="switch_type", aggfunc=lambda x: sum(x == "strict_repeat")
            ),
            ambiguous_switch_count=pd.NamedAgg(
                column="switch_type", aggfunc=lambda x: sum(x == "ambiguous_switch")
            ),
            strict_switch_count=pd.NamedAgg(
                column="switch_type", aggfunc=lambda x: sum(x == "strict_switch")
            ),
            num_of_cards_played_in_turn=pd.NamedAgg(
                column="event_name", aggfunc=lambda x: sum(x == "user_played_card")
            ),
            x_variable_switches=pd.NamedAgg(column="changed_x_val", aggfunc="count"),
            num_cards_in_hand_at_start_of_turn=pd.NamedAgg(column="p1_hand_size_pre_action", aggfunc="first"),
            num_play_not_allowed=pd.NamedAgg(
                column="event_name", aggfunc=lambda x: sum(x == "play_not_allowed")
            ),
            max_possible_cards_playable=pd.NamedAgg(column="best_play_length", aggfunc=max),
            first_match_in_turn=pd.NamedAgg(column="match_type", aggfunc="first"),
        )
    ).reset_index()

    time_to_first_move_df = (
        event_df.groupby(["user_id", "user_game_index", "user_turn_start_index"])
        .apply(lambda x: round(x.user_action_time.min() - x.user_turn_start_time.min(), 4))
        .reset_index()
        .rename(columns={0: "time_to_first_action_seconds"})
    )

    total_turn_time_df = (
        event_df.groupby(["user_id", "user_game_index", "user_turn_start_index"])
        .apply(
            lambda x: round(x.user_turn_end_time.min() - x.user_turn_start_time.min(), 4),
        )
        .reset_index()
        .rename(columns={0: "total_turn_time_seconds"})
    )

    return pd.merge(agg_df, time_to_first_move_df, how="left").merge(total_turn_time_df, how="left")


def calc_switches(input_df):
    event_df = input_df.copy()
    user_played_card_df = event_df[
        (event_df.event_name == "user_played_card") & (~event_df.card.str.contains("white", na=False))
    ].copy()
    match_types = ["color", "value", "algebraic"]
    for match_type in match_types:
        user_played_card_df[f"previous_match_{match_type}"] = user_played_card_df[
            f"match_{match_type}"
        ].shift(1, fill_value=False)
        user_played_card_df[f"previous_match_{match_type}"] == user_played_card_df[f"match_{match_type}"]

    match_columns = [f"match_{match_type}" for match_type in match_types]
    previous_columns = [f"previous_match_{match_type}" for match_type in match_types]
    user_played_card_df["switch_type"] = user_played_card_df.apply(
        lambda x: calc_switch_type(x[match_columns].values, x[previous_columns].values),
        axis=1,
    )
    user_played_card_df["match_type"] = user_played_card_df.apply(
        lambda x: "_".join([match_type for match_type in match_types if x[f"match_{match_type}"] is True]),
        axis=1,
    )
    # check for missed list elements user_played_card_df.applymap(lambda x: type(x)==list).max()

    return pd.merge(
        input_df,
        user_played_card_df.drop(["p1_hand", "best_play", "board", "p0_hand"], axis=1, errors="ignore"),
        how="left",
    )


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
        and (all([str(a) != str(b) for a, b in zip(current_matches, previous_matches) if a == 1]))
    ):
        return "strict_switch"
    elif (
        (sum(current_matches) >= 1)
        and (sum(previous_matches) >= 1)
        and (sum([str(a) == str(b) for a, b in zip(current_matches, previous_matches)]) >= 1)
    ):
        return "ambiguous_switch"
    else:
        return "other"
