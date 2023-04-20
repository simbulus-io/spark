# %% [markdown]
# # Flatten Equivacard event data

# %%
import plotnine
import pandas as pd
import numpy as np
import json
from datetime import datetime
import os

# %%
json_file_path = "./data/middie_full.json"

# %%
with open(json_file_path) as f:
    event_json = json.load(f)

# %% [markdown]
# ## Preview event shape

# %%
event_json[0:3]

# %%
event_json[-3:]

# %%
json_df = pd.read_json(json_file_path, dtype= {'timestamp': int} )

# %% [markdown]
# ## Align timestamps

# %%
json_df.timestamp.values[0]

# %%
json_df['server_timestamp'].describe()

# %%
json_df['timestamp'].describe()

# %%
json_df[json_df['timestamp']>0]['timestamp'].describe()

# %%
json_df[json_df['timestamp']>10.0**16]['timestamp'].describe()

# %%
print(datetime.utcfromtimestamp(1.662995e+15/1000000).strftime('%Y-%m-%d %H:%M:%S'))

# %%
1.662995e+15/(10**12)

# %%
print(datetime.utcfromtimestamp(json_df.timestamp.values.max()/1000000000).strftime('%Y-%m-%d %H:%M:%S'))

# %%
print(datetime.utcfromtimestamp(json_df.server_timestamp.min()/1000).strftime('%Y-%m-%d %H:%M:%S'))

# %%
print(datetime.utcfromtimestamp(json_df.server_timestamp.max()/1000).strftime('%Y-%m-%d %H:%M:%S'))

# %%
def align_unix_convention(x):
    if not np.isnan(x.server_timestamp):
        return x.server_timestamp/1000 
    else:
        ts = json_df.timestamp.values.max()
        above_12 = np.floor(np.log10(json_df.timestamp.values.max())-12)
        return ts/(10**(above_12+3))
    

# %%
json_df['unix_timestamp_combined'] = json_df.apply(lambda x: align_unix_convention(x), axis=1)

# %%
json_df['timestamp_combined'] = json_df.apply(lambda x: datetime.utcfromtimestamp(x.unix_timestamp_combined), axis=1)

# %%
json_df['unix_timestamp_combined'].describe()

# %%
print(json_df.timestamp_combined.min())

# %%
print(json_df.timestamp_combined.max())

# %%
json_df.sample(5)

# %% [markdown]
# ## Review event distribution across field categories

# %%
json_df.groupby('activity').count()

# %%
json_df.groupby('event_name').count()

# %%
json_df.groupby('bucket').count()

# %%
json_df.groupby('activity')['user_id'].nunique()

# %%
equiv_events_df = json_df[json_df.activity.isin(['EQUIVACARDS'])]

# %%
equiv_events_df.shape

# %% [markdown]
# ## Flatten object columns

# %%
payload_df = pd.json_normalize(equiv_events_df.payload)

# %%
payload_df.sample(5)

# %% [markdown]
# `best_play`, `board` and `p1_hand` are objects that are not easy to process as is

# %%
payload_df[payload_df.best_play.notna()].sample(5).best_play.values

# %% [markdown]
# `best_play` is a complex object and would require targeted processing to pull out value
# 
# Length of the best play and first move of the best play seem likely useful 

# %%
best_play_df = payload_df.best_play.apply(pd.Series) 

# %%
best_play_df.columns = [f"best_play_turn_{item}" for item in best_play_df.columns]

# %%
best_play_df[best_play_df.best_play_turn_0.notna()].head()

# %%
# best_play_df["best_play_length"] = best_play_df.count(axis=1)

# %%
best_play_df[best_play_df.best_play_turn_0.notna()].head()

# %%
best_play_0_df = pd.json_normalize(best_play_df.best_play_turn_0,errors='ignore')

# %%
best_play_0_df

# %%
best_play_0_df.columns = [f"best_play_turn_0_{item}" for item in best_play_0_df.columns]

# %%
payload_df[payload_df.board.notna()].sample(5).board.values

# %%
board_df = payload_df.board.apply(pd.Series) 

# %%
board_df.columns = ["board_left_card", "board_right_card"]

# %%
board_df[board_df.board_right_card.notna()].sample(5)

# %%
payload_df[payload_df.p1_hand.notna()].sample(5).p1_hand.values

# %%
p1_hand_df = payload_df.p1_hand.apply(pd.Series) 

# %%
p1_hand_df.columns = [f"p1_hand_card_{item}" for item in p1_hand_df.columns]

# %%
p1_hand_df['p1_hand_size'] = p1_hand_df.count(axis=1)

# %%
p1_hand_df[p1_hand_df.p1_hand_card_0.notna()].sample(5)

# %%
equiv_flat_df = pd.concat([
    equiv_events_df.reset_index(drop=True), 
    payload_df.reset_index(drop=True),
    best_play_df.reset_index(drop=True),
    best_play_0_df.reset_index(drop=True),
    board_df.reset_index(drop=True),
    p1_hand_df.reset_index(drop=True)
], axis=1)

# %%
equiv_flat_df.sample(5)

# %% [markdown]
# ## Correct Connect the Drops labels

# %%
game_temp_df = equiv_flat_df[equiv_flat_df.event_name.isin(["launched_connect_the_drops", "launched_equivacards", "user_won", "user_lost"])].sort_values(by='timestamp_combined')

# %%
game_temp_df = equiv_flat_df.sort_values(by='timestamp_combined')

# %%
game_temp_df['game_start'] = game_temp_df.event_name.apply(lambda x: x in ['launched_connect_the_drops', 'launched_equivacards'])

# %%
def correct_launch_activity(x):
    if x == 'launched_connect_the_drops':
        return "CONNECT_THE_DROPS"
    elif x ==  'launched_equivacards':
        return "EQUIVACARDS"

# %%
game_temp_df['corrected_activity'] = game_temp_df.event_name.apply(correct_launch_activity)

# %%
game_temp_df['game_end'] = game_temp_df.event_name.apply(lambda x: x in ['user_won', 'user_lost'])

# %%
game_temp_df['user_launch_index'] = game_temp_df.groupby('user_id').game_start.cumsum()
game_temp_df['user_game_index'] = game_temp_df.groupby('user_id').game_end.cumsum()

# %%
def correct_activity(x):
    if x == 'launched_connect_the_drops':
        return "CONNECT_THE_DROPS"
    elif x ==  'launched_equivacards':
        return "EQUIVACARDS"

# %%
game_temp_df['corrected_activity'] = game_temp_df.event_name.apply(lambda x: correct_activity(x))

# %%
game_by_launch_df = game_temp_df[game_temp_df['corrected_activity'].notna()][['user_id', "user_launch_index","corrected_activity"]]

# %%
game_by_launch_df.sample(5)

# %%
corrected_activity_events_df = pd.merge(game_temp_df.drop('corrected_activity', axis=1), game_by_launch_df, on=["user_id", "user_launch_index"])

# %%
corrected_activity_events_df.groupby('corrected_activity')['user_id'].nunique()

# %%
corrected_equivacards_events = corrected_activity_events_df[corrected_activity_events_df.corrected_activity=="EQUIVACARDS"]

# %%
corrected_equivacards_events.describe()

# %%
corrected_equivacards_events.groupby('event_name').count()

# %%
corrected_equivacards_events.groupby('event_name')['user_id'].nunique()

# %%
corrected_equivacards_events.dtypes

# %%
corrected_equivacards_events.columns.sort_values()

# %%
os.makedirs('./data', exist_ok=True)
with open('./data/results.json', 'w+') as f:
    json.dumps(corrected_equivacards_events.reset_index().to_json("records"))

# %% [markdown]
# ## Review of game time and distribution of events per game 

# %%
from plotnine import ggplot, geom_point, aes, stat_smooth, facet_wrap, scale_x_date, geom_line,facet_grid, theme, element_text, labs, element_blank, ggtitle, geom_bar

# %%
(ggplot(
   corrected_equivacards_events, aes('timestamp_combined', 'factor(user_id)', color= 'factor(user_id)'))
+ geom_point(show_legend=False)
 + geom_line(show_legend=False)
  + labs(x= "date", y="User Id")
 + ggtitle("Game by user_id vs date")
 + theme(figure_size=(6, 4), axis_text_x=element_text(rotation=90, hjust=1), ) 
)

# %%
(ggplot(
   corrected_equivacards_events, aes('user_game_index', fill= 'factor(event_name)'))
+ geom_bar(stat='count', position= 'stack')
 + facet_grid('user_id ~', scales ='free', )
 + theme(figure_size=(4, 10), axis_text_x=element_text(rotation=90, hjust=1), strip_text_y = element_text(angle = 0)) 
 + labs(x= "User Game Index", y="Event count")
 + ggtitle("Game events by user_id")
)

# %% [markdown]
# ## Cursory comparison of best move 0 to taken move 0

# %%
corrected_equivacards_events['previous_best_move_0'] = corrected_equivacards_events.groupby('user_id').best_play_turn_0_label.shift(2).apply(lambda x: x.replace(',', '.') if type(x)==str else None ) 
corrected_equivacards_events['made_best_move_0'] = corrected_equivacards_events.apply(lambda x: x.previous_best_move_0==x.card, axis =1) 

# %%
corrected_equivacards_events[['event_name','card', 'best_play_turn_0_label', 'previous_best_move_0', 'made_best_move_0']].head(15)

# %%
corrected_equivacards_events[corrected_equivacards_events.event_name=='user_turn'].groupby(['user_id', 'made_best_move_0'])['_id'].count()

# %%
corrected_equivacards_events


