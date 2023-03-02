import argparse
import csv
import json
import os
from datetime import datetime
from pathlib import Path

def eval_card(x,expr):
    if expr=='1':
        return(1)
    elif expr=='2':
        return(2)
    elif expr=='3':
        return(3)
    elif expr=='4':
        return(4)
    elif expr=='5':
        return(5)
    elif expr=='6':
        return(6)
    elif expr=='7':
        return(7)
    elif expr=='8':
        return(8)
    elif expr=='x':
        return(x)
    elif expr=='x+x':
        return(x+x)
    elif expr=='x+1':
        return(x+1)
    elif expr=='x+2':
        return(x+2)
    elif expr=='9-x':
        return(9-x)
    elif expr=='2x':
        return(2*x)
    raise(f'unexpected card expression {expr}')
          

def main(src, outdir):

    evts = None
    with open(src,'r') as fin:
        evts = json.load(fin)
    print(f'loaded {len(evts)} events from {src}')

    # - - - - - - - - - - - - Data cleanup - - - - - - - - - - - - 
    
    # sort the array of events by their unix epoch timestamp
    evts = (sorted(evts, key=lambda e: int(e['timestamp']) ))

    irrelevant_keys = ["_id", "browser_session_id", "product", "activity", "bucket"]

    for i,e in enumerate(evts):
        # get a string formated local time in milliseconds from the timestamp. We are running this
        # in MST (local) and outputting for PDT (since the test was run in PDT and PDT=MST=GMT-7)
        e['event_time'] = datetime.fromtimestamp(int(e['timestamp'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' PDT'
        for k in irrelevant_keys:
            e.pop(k,None) # strip the irrelevant data from the event blobs

    # - - - - - - - - - - - - separate events by user id  - - - - - - - - - - - - 

    uid_event_map = {}

    for e in evts:
        uid = e['user_id']
        uid_event_map.setdefault(uid, []).append(e)

    # - - - - - - - - - - - - process the events by user and game  - - - - - - - - - - - - 

    for uid, uevts in uid_event_map.items():
        games = []
        events_by_game = []

        last_gamestate = None
        cur_turn = None
        cur_game = None
        cur_game_events = None

        def clear_game():
            nonlocal last_gamestate
            nonlocal cur_turn
            nonlocal cur_game
            nonlocal cur_game_events
            last_gamestate = None
            cur_turn = None
            cur_game = {}
            cur_game_events = []
                
        clear_game();
            
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
            
        for e in uevts:
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
                cur_turn = e["payload"];
                # "best_play_length": 6,
                # "best_play": [
            elif (e["event_name"] == "user_played_card") or (e["event_name"] == "user_drew_card"):
                p = e["payload"]
                if last_gamestate == None:
                    p["options"] = "unknown"
                else:
                    gs = last_gamestate
                    if gs["whose_turn"]!=1:
                        raise Exception(f'whose_turn unexpected in {e}, {gs}')
                    x = int(gs["board"][0][-1:])
                    color = gs["board"][1].split(".")[0]
                    val   = gs["board"][1].split(".")[1]
                    aval  = eval_card(x,val);
                    opts = {"can_match_color": False, "can_match_value": False, "can_match_algebraic": False}
                    for c in gs["p1_hand"]:
                        c_color = c.split(".")[0]
                        c_val   = c.split(".")[1]
                        if (c_color == "white"):
                            c_x = int(c[-1:])
                            if (c_x != x):
                                # Card would change x value
                                # To Do: determine whether playing the white card makes any other cards playable
                                # that are not currently playable. We would need to move some of the logic here into
                                # functions, and have a function that returns a list of the playable (non-white) cards
                                # given a board position. Then call that function for our hand with the current board x
                                # value, and then again with this current card, c, played. 
                                False
                        else:
                            c_aval  = eval_card(x,c_val);
                            if color==c_color: opts["can_match_color"] = True
                            if val==c_val:     opts["can_match_value"] = True
                            elif aval==c_aval: opts["can_match_algebraic"] = True
                    p["options"] = opts
                if e["event_name"] == "user_played_card":
                    cur_game["n_cards_played"] = cur_game.get("n_cards_played", 0) + 1
                elif e["event_name"] == "user_drew_card":
                    cur_game["n_cards_drawn"] = cur_game.get("n_cards_drawn", 0) + 1
            elif (e["event_name"] == "play_not_allowed"):
                # To Do: tabulate data on the user trying to play cards that can't be played.
                # crosstab on is_algebraic_card
                is_algebraic_card = ("x" in e["payload"]["card"])
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
                
            cur_game_events.append(e);
            
        wrap_game()
        print(f'Participant {uid} played {len(games)} games') 
        print(f'   finished {sum("finished" in g and (g["finished"] == True) for g in games)} ') 
        print(f'        won {sum("won" in g and (g["won"] == True) for g in games)} ') 
        print(f'       lost {sum("won" in g and (g["won"] == False) for g in games)} ') 

        for i,g in enumerate(games):
            game_dir = f'{outdir}/{uid}/game_{i+1:02}'
            Path(game_dir).mkdir(parents=True, exist_ok=True)
            with open(f'{game_dir}/events.json','w') as fout:
                print(json.dumps(events_by_game[i], indent=2), file=fout)
            
        with open(f'{outdir}/{uid}/games.csv', 'w') as fout:
            writer = csv.DictWriter(fout, set().union(*games))
            writer.writeheader()
            writer.writerows(games)


                    
    # - - - - - - - - - - - - write out events by user id  - - - - - - - - - - - -
    
    for k,v in uid_event_map.items():
        Path(f'{outdir}/{k}').mkdir(parents=True, exist_ok=True)
        with open(f'{outdir}/{k}/events.json','w') as fout:
            print(json.dumps(v, indent=2), file=fout)

            

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="diariarize a file")
    parser.add_argument("-f", "--file", type=str, required=True, 
                        help="the source json file")
    parser.add_argument("-o", "--output", type=str, required=True, 
                        help="name of the output directory")
    args = parser.parse_args()
    main(args.file, args.output)
    
    
