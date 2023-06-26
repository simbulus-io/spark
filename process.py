# -*- coding: utf-8 -*-
import argparse
import csv
import json
import os
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path


def eval_card(x, expr):
    if expr == "1":
        return 1
    elif expr == "2":
        return 2
    elif expr == "3":
        return 3
    elif expr == "4":
        return 4
    elif expr == "5":
        return 5
    elif expr == "6":
        return 6
    elif expr == "7":
        return 7
    elif expr == "8":
        return 8
    elif expr == "x":
        return x
    elif expr == "x+x":
        return x + x
    elif expr == "x+1":
        return x + 1
    elif expr == "x+2":
        return x + 2
    elif expr == "9-x":
        return 9 - x
    elif expr == "2x":
        return 2 * x
    raise (f"unexpected card expression {expr}")


def main(src, outdir):
    evts = None
    with open(src, "r") as fin:
        evts = json.load(fin)
    print(f"loaded {len(evts)} events from {src}")

    # - - - - - - - - - - - - Data cleanup - - - - - - - - - - - -

    # sort the array of events by their unix epoch timestamp
    evts = sorted(evts, key=lambda e: int(e["timestamp"]))

    irrelevant_keys = ["_id", "browser_session_id", "product", "bucket"]

    for i, e in enumerate(evts):
        # get a string formated local time in milliseconds from the timestamp. We are running this
        # in MST (local) and outputting for PDT (since the test was run in PDT and PDT=MST=GMT-7)
        e["event_time"] = (
            datetime.fromtimestamp(int(e["timestamp"])).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]
            + " PDT"
        )
        for k in irrelevant_keys:
            e.pop(k, None)  # strip the irrelevant data from the event blobs

    # - - - - - - - - - - - - separate events by user id  - - - - - - - - - - - -

    uid_event_map = {}

    for e in evts:
        uid = e["user_id"]
        uid_event_map.setdefault(uid, []).append(e)

    # - - - - - - - - - - - - process the events by user and game  - - - - - - - - - - - -

    # Iterate thru all games in JSON file
    for uid, uevts in uid_event_map.items():
        # Arrays for storing data from game
        games = []
        turns = []
        events_by_game = []

        # Variables for game and turn stats dataframes
        game_stats_keys = [
            "user_id",
            "start_time",
            "end_time",
            "started",
            "finished",
            "n_turns",
            "n_cards_drawn",
            "n_cards_played",
            "can_match_color",
            "can_match_value",
            "can_match_algebraic",
            "matched_color",
            "matched_value",
            "matched_algebraic",
            "won",
            "play_again",
        ]
        turn_stats_keys = [
            "user_id",
            "event_time",
            "event_name",
            "whose_turn",
            "board",
            "p1_hand",
            "p2_ncards",
            "card",
            "value",
            "can_match_color",
            "can_match_value",
            "can_match_algebraic",
            "matched_color",
            "matched_value",
            "matched_algebraic",
        ]

        last_gamestate = None
        cur_turn = None  # Dictionary that records stats for current turn
        cur_game = None  # Dictionary that records stats for current game
        cur_game_events = None  # List for inputting data into JSON files

        # Clear previous game data before processing next game
        def clear_game():
            nonlocal last_gamestate
            nonlocal cur_turn
            nonlocal cur_game
            nonlocal cur_game_events
            last_gamestate = None
            cur_turn = {}
            cur_game = {}
            cur_game_events = []

        clear_game()

        # End current game processing and store data into appropriate arrays
        def wrap_game():
            nonlocal cur_game
            nonlocal cur_game_events
            nonlocal games
            nonlocal events_by_game
            nonlocal last_gamestate
            nonlocal cur_turn
            nonlocal uid
            if len(cur_game.keys()) > 0:
                if "won" not in cur_game:
                    cur_game["finished"] = False
                cur_game["user_id"] = uid
                games.append(cur_game)
                events_by_game.append(cur_game_events)
            clear_game()

        # Iterate thru each event in current game, and calculate and record game/turn stats
        for e in uevts:
            cur_turn = {}
            cur_turn["user_id"] = e["user_id"]
            cur_turn["event_time"] = e["event_time"]
            cur_turn["event_name"] = e["event_name"]

            # Event-specific commands
            if e["event_name"] == "launched_equivacards":
                wrap_game()
            elif e["event_name"] == "game_state_changed":
                if e["payload"]["whose_turn"] == 1:
                    last_gamestate = e["payload"]
            elif e["event_name"] == "user_turn":
                cur_game["started"] = True
                cur_game["n_turns"] = cur_game.get("n_turns", 0) + 1
                if "start_time" not in cur_game:
                    cur_game["start_time"] = e["event_time"]
                # cur_turn = e["payload"]; <--- This line wasn't being used
                # "best_play_length": 6,
                # "best_play": [
            elif (e["event_name"] == "user_played_card") or (
                e["event_name"] == "user_drew_card"
            ):
                p = e["payload"]
                if last_gamestate == None:
                    p["options"] = "unknown"
                else:
                    gs = last_gamestate
                    if gs["whose_turn"] != 1:
                        raise Exception(f"whose_turn unexpected in {e}, {gs}")

                    cur_turn["whose_turn"] = gs["whose_turn"]
                    cur_turn["board"] = gs["board"]
                    cur_turn["p1_hand"] = gs["p1_hand"]
                    cur_turn["p2_ncards"] = gs["p2_ncards"]

                    x = int(gs["board"][0][-1:])
                    color = gs["board"][1].split(".")[0]
                    val = gs["board"][1].split(".")[1]
                    aval = eval_card(x, val)
                    opts = {
                        "can_match_color": False,
                        "can_match_value": False,
                        "can_match_algebraic": False,
                    }
                    for c in gs["p1_hand"]:
                        c_color = c.split(".")[0]
                        c_val = c.split(".")[1]
                        if c_color == "white":
                            c_x = int(c[-1:])
                            if c_x != x:
                                # Card would change x value
                                # To Do: determine whether playing the white card makes any other cards playable
                                # that are not currently playable. We would need to move some of the logic here into
                                # functions, and have a function that returns a list of the playable (non-white) cards
                                # given a board position. Then call that function for our hand with the current board x
                                # value, and then again with this current card, c, played.
                                False
                        else:
                            c_aval = eval_card(x, c_val)
                            if color == c_color and not opts["can_match_color"]:
                                cur_game["can_match_color"] = (
                                    cur_game.get("can_match_color", 0) + 1
                                )
                                opts["can_match_color"] = cur_turn[
                                    "can_match_color"
                                ] = True
                            if val == c_val and not opts["can_match_value"]:
                                cur_game["can_match_value"] = (
                                    cur_game.get("can_match_value", 0) + 1
                                )
                                opts["can_match_value"] = cur_turn[
                                    "can_match_value"
                                ] = True
                            elif aval == c_aval and not opts["can_match_algebraic"]:
                                cur_game["can_match_algebraic"] = (
                                    cur_game.get("can_match_algebraic", 0) + 1
                                )
                                opts["can_match_algebraic"] = cur_turn[
                                    "can_match_algebraic"
                                ] = True
                    p["options"] = opts
                if e["event_name"] == "user_played_card":
                    cur_turn["card"] = p["card"]
                    cur_turn["value"] = p["value"]
                    cur_game["n_cards_played"] = cur_game.get("n_cards_played", 0) + 1
                    if ("match_color" in p.keys()) and p["match_color"] == True:
                        cur_turn["matched_color"] = True
                        cur_game["matched_color"] = cur_game.get("matched_color", 0) + 1
                    if ("match_value" in p.keys()) and p["match_value"] == True:
                        cur_turn["matched_value"] = True
                        cur_game["matched_value"] = cur_game.get("matched_value", 0) + 1
                    if ("match_algebraic" in p.keys()) and p["match_algebraic"] == True:
                        cur_turn["matched_algebraic"] = True
                        cur_game["matched_algebraic"] = (
                            cur_game.get("matched_algebraic", 0) + 1
                        )
                elif e["event_name"] == "user_drew_card":
                    cur_game["n_cards_drawn"] = cur_game.get("n_cards_drawn", 0) + 1
            elif e["event_name"] == "play_not_allowed":
                # To Do: tabulate data on the user trying to play cards that can't be played.
                # crosstab on is_algebraic_card
                is_algebraic_card = "x" in e["payload"]["card"]
                False
            elif e["event_name"] == "user_won":
                cur_game["end_time"] = e["event_time"]
                cur_game["finished"] = True
                cur_game["won"] = True
            elif e["event_name"] == "user_lost":
                cur_game["end_time"] = e["event_time"]
                cur_game["finished"] = True
                cur_game["won"] = False
            elif e["event_name"] == "play_again_yes":
                cur_game["play_again"] = True
                wrap_game()
            elif e["event_name"] == "play_again_no":
                cur_game["play_again"] = False
                wrap_game()

            cur_game_events.append(e)
            turns.append(cur_turn)

        # Print a few game stats to terminal
        wrap_game()
        print(f"Participant {uid} played {len(games)} games")
        print(
            f'   finished {sum("finished" in g and (g["finished"] == True) for g in games)} '
        )
        print(f'        won {sum("won" in g and (g["won"] == True) for g in games)} ')
        print(f'       lost {sum("won" in g and (g["won"] == False) for g in games)} ')

        # Write game events to events.json
        for i, g in enumerate(games):
            game_dir = f"{outdir}/{uid}/game_{i+1:02}"
            Path(game_dir).mkdir(parents=True, exist_ok=True)
            with open(f"{game_dir}/events.json", "w") as fout:
                print(json.dumps(events_by_game[i], indent=2), file=fout)

        # - - - - - - Write game_stats dataframe to game_stats.csv - - - - - -
        game_stats = pd.DataFrame(columns=game_stats_keys)
        # Append each dict in games to game_stats dataframe
        for dict in games:
            df_dict = pd.DataFrame([dict])
            game_stats = pd.concat([game_stats, df_dict], ignore_index=True)
        # Write to csv file
        game_stats.to_csv(f"{outdir}/{uid}/game_stats.csv")

        # - - - - - - Write turn_stats dataframe to turn_stats.csv - - - - - -
        turn_stats = pd.DataFrame(columns=turn_stats_keys)
        # Append each dict in turns to turn_stats dataframe
        for dict in turns:
            df_dict = pd.DataFrame([dict])
            turn_stats = pd.concat([turn_stats, df_dict], ignore_index=True)
        # Write to csv file
        turn_stats.to_csv(f"{outdir}/{uid}/turn_stats.csv")

    # - - - - - - - - - - - - write out events by user id  - - - - - - - - - - - -

    for k, v in uid_event_map.items():
        Path(f"{outdir}/{k}").mkdir(parents=True, exist_ok=True)
        with open(f"{outdir}/{k}/events.json", "w") as fout:
            print(json.dumps(v, indent=2), file=fout)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="diariarize a file")
    parser.add_argument(
        "-f", "--file", type=str, required=True, help="the source json file"
    )
    parser.add_argument(
        "-o", "--output", type=str, required=True, help="name of the output directory"
    )
    args = parser.parse_args()
    main(args.file, args.output)
