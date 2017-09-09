import os
import pandas as pd
import xmltodict
import json
pd.options.display.max_columns = None
import sys

'''
Script to extract data from raw xml files, structure them in a table format and import them into a data source

To test run
ipython mlb_etl.py /Volumes/LACIE\ SHARE/mlb_data/year_2015
'''

class MLBExtractor():
    

    def __init__(self, path_to_year):
        self.base_file_path = path_to_year
        self.game_paths = {}
        for month in os.listdir(path_to_year):
            month_path = path_to_year + '/' + month
            for day in os.listdir(month_path):
                day_path = month_path + '/' + day
                for game in os.listdir(day_path):
                    self.game_paths[game] = day_path + '/' + game

    def build_batter_df(self, game_path, game_id):
        json_batters = []
        batter_path = game_path + '/' + 'batters'

        for player in os.listdir(batter_path):
            xml_path = batter_path + '/' + player
            player_json = xml_path_to_json(xml_path)
            json_batters.append(player_json)

        batters = []

        for x in json_batters:
            temp_dict = {}
            for y in x.get('Player').iteritems():
                if isinstance(y[1], dict):
                    stat_name = y[0]
                    for z in y[1].iteritems():
                        temp_dict[y[0] + '_' + z[0]] = z[1]
            
            z = x.get('Player').copy()
            z.update(temp_dict)
            batters.append(z)

        df_batter = pd.DataFrame(batters)

        df_batter.columns = ['bats', 'current_position', 'dob', 'first_name', 'height', 'id',
        'jersey_number', 'last_name', 'pos', 'team', 'throws', 'type', 'weight', 'Empty',
        'Empty_ab', 'Empty_avg', 'Empty_bb', 'Empty_cs', 'Empty_h', 'Empty_hr', 'Empty_ops',
        'Empty_r', 'Empty_rbi', 'Empty_sb', 'Empty_so', 'Loaded', 'Loaded_ab', 'Loaded_avg',
        'Loaded_bb', 'Loaded_cs', 'Loaded_h', 'Loaded_hr', 'Loaded_ops', 'Loaded_r',
        'Loaded_rbi', 'Loaded_sb', 'Loaded_so', 'Men_On', 'Men_On_ab', 'Men_On_avg', 
        'Men_On_bb', 'Men_On_cs', 'Men_On_h', 'Men_On_hr', 'Men_On_ops', 'Men_On_r', 
        'Men_On_rbi', 'Men_On_sb', 'Men_On_so', 'Pitch', 'Pitch_hates', 'Pitch_loves', 
        'RISP', 'RISP_ab', 'RISP_avg', 'RISP_bb', 'RISP_cs', 'RISP_h', 'RISP_hr', 'RISP_ops', 
        'RISP_r', 'RISP_rbi', 'RISP_sb', 'RISP_so', 'Team', 'Team_ab', 'Team_avg', 'Team_bb', 
        'Team_cs', 'Team_des', 'Team_h', 'Team_hr', 'Team_ops', 'Team_r', 'Team_rbi', 
        'Team_sb', 'Team_so', 'atbats', 'atbats_ab', 'career', 'career_ab', 'career_avg', 
        'career_bb', 'career_cs', 'career_h', 'career_hr', 'career_ops', 'career_r', 
        'career_rbi', 'career_sb', 'career_so', 'faced', 'faced_pitch', 'month', 'month_ab', 
        'month_avg', 'month_bb', 'month_cs', 'month_des', 'month_h', 'month_hr', 'month_ops', 
        'month_r', 'month_rbi', 'month_sb', 'month_so', 'season', 'season_ab', 'season_avg', 
        'season_bb', 'season_cs', 'season_h', 'season_hr', 'season_ops', 'season_r', 
        'season_rbi', 'season_sb', 'season_so', 'vs_LHP', 'vs_LHP_ab', 'vs_LHP_avg', 
        'vs_LHP_bb', 'vs_LHP_cs', 'vs_LHP_h', 'vs_LHP_hr', 'vs_LHP_ops', 'vs_LHP_r', 
        'vs_LHP_rbi', 'vs_LHP_sb', 'vs_LHP_so', 'vs_P', 'vs_P5', 'vs_P5_ab', 'vs_P5_avg', 
        'vs_P5_bb', 'vs_P5_cs', 'vs_P5_des', 'vs_P5_h', 'vs_P5_hr', 'vs_P5_ops', 'vs_P5_r', 
        'vs_P5_rbi', 'vs_P5_sb', 'vs_P5_so', 'vs_P_ab', 'vs_P_avg', 'vs_P_bb', 'vs_P_cs', 
        'vs_P_des', 'vs_P_h', 'vs_P_hr', 'vs_P_ops', 'vs_P_r', 'vs_P_rbi', 'vs_P_sb', 
        'vs_P_so', 'vs_RHP', 'vs_RHP_ab', 'vs_RHP_avg', 'vs_RHP_bb', 'vs_RHP_cs', 'vs_RHP_h',
        'vs_RHP_hr', 'vs_RHP_ops', 'vs_RHP_r', 'vs_RHP_rbi', 'vs_RHP_sb', 'vs_RHP_so']

        df_batter = df_batter.drop(['Empty','Loaded','Men_On','Pitch','RISP','Team','career',
            'atbats','atbats_ab','faced','faced_pitch','month','season',
            'vs_LHP','vs_P','vs_RHP','vs_P5'], axis = 1)

        return df_batter

    def build_pitcher_df(self, game_path, game_id):
        json_pitchers = []
        pitcher_path = game_path + '/' + 'pitchers'

        for player in os.listdir(pitcher_path):
            xml_path = pitcher_path + '/' + player
            player_json = xml_path_to_json(xml_path)
            json_pitchers.append(player_json)

        pitchers = []

        for x in json_pitchers:
            temp_dict = {}
            for y in x.get('Player').iteritems():
                if isinstance(y[1], dict):
                    stat_name = y[0]
                    for z in y[1].iteritems():
                        temp_dict[y[0] + '_' + z[0]] = z[1]
            
            z = x.get('Player').copy()
            z.update(temp_dict)
            pitchers.append(z)


        df_pitcher = pd.DataFrame(pitchers)
        
        df_pitcher.columns = ['bats', 'dob', 'first_name', 'height', 'id', 'jersey_number',
        'last_name', 'pos', 'team', 'throws', 'type', 'weight', 'Empty', 'Empty_ab', 'Empty_avg',
        'Empty_bb', 'Empty_era', 'Empty_h', 'Empty_hr', 'Empty_ip', 'Empty_rbi', 'Empty_so',
        'Empty_whip', 'Loaded', 'Loaded_ab', 'Loaded_avg', 'Loaded_bb', 'Loaded_era','Loaded_h',
        'Loaded_hr', 'Loaded_ip', 'Loaded_rbi', 'Loaded_so', 'Loaded_whip', 'Men_On', 'Men_On_ab',
        'Men_On_avg', 'Men_On_bb', 'Men_On_era', 'Men_On_h', 'Men_On_hr', 'Men_On_ip',
        'Men_On_rbi', 'Men_On_so', 'Men_On_whip', 'Month', 'Month_ab', 'Month_avg', 'Month_bb',
        'Month_des', 'Month_era', 'Month_h', 'Month_hr', 'Month_ip', 'Month_rbi', 'Month_so',
        'Month_whip', 'Pitch', 'Pitch_out', 'RISP', 'RISP_ab', 'RISP_avg', 'RISP_bb', 'RISP_era',
        'RISP_h', 'RISP_hr', 'RISP_ip', 'RISP_rbi', 'RISP_so', 'RISP_whip', 'Team', 'Team_ab',
        'Team_avg', 'Team_bb', 'Team_des', 'Team_era', 'Team_h', 'Team_hr', 'Team_ip', 'Team_rbi',
        'Team_so', 'Team_whip', 'career', 'career_ab', 'career_avg', 'career_bb', 'career_era',
        'career_h', 'career_hr', 'career_ip', 'career_l', 'career_rbi', 'career_so', 'career_sv',
        'career_w', 'career_whip', 'season', 'season_ab', 'season_avg', 'season_bb', 'season_era',
        'season_h', 'season_hr', 'season_ip', 'season_l', 'season_rbi', 'season_so', 'season_sv', 
        'season_w', 'season_whip', 'vs_B', 'vs_B5', 'vs_B5_ab', 'vs_B5_avg', 'vs_B5_bb', 'vs_B5_des', 
        'vs_B5_era', 'vs_B5_h', 'vs_B5_hr', 'vs_B5_ip', 'vs_B5_rbi', 'vs_B5_so', 'vs_B5_whip', 
        'vs_B_ab', 'vs_B_avg', 'vs_B_bb', 'vs_B_des', 'vs_B_era', 'vs_B_h', 'vs_B_hr', 'vs_B_ip',
        'vs_B_rbi', 'vs_B_so', 'vs_B_whip', 'vs_LHB', 'vs_LHB_ab', 'vs_LHB_avg', 'vs_LHB_bb', 
        'vs_LHB_era', 'vs_LHB_h', 'vs_LHB_hr', 'vs_LHB_ip', 'vs_LHB_rbi', 'vs_LHB_so', 'vs_LHB_whip', 
        'vs_RHB', 'vs_RHB_ab', 'vs_RHB_avg', 'vs_RHB_bb', 'vs_RHB_era', 'vs_RHB_h', 'vs_RHB_hr', 
        'vs_RHB_ip', 'vs_RHB_rbi', 'vs_RHB_so', 'vs_RHB_whip']
        
        df_pitcher = df_pitcher.drop(['Empty','Loaded','Men_On','Month','Pitch','Pitch_out','RISP',
        'Team','career','season','vs_B','vs_B5','vs_LHB','vs_RHB',], axis = 1)

        return df_pitcher


    def build_inning_df(self, game_path, game_id):
        inning_path = game_path + '/' + 'inning' + '/' + 'inning_all.xml'
        inning_json = xml_path_to_json(inning_path)

        bat_list = []
        action_list = []
        pitch_list = []
        runner_list = []

        for inning in inning_json.get('game').get('inning'):
            for x in inning.get('bottom').iteritems():
                if x[0] == 'atbat':
                    bat_list.append(x[1])
                elif x[0] == 'action':
                    if type(x[1]) is dict:
                        action_list.append(x[1])
                    else:
                        for y in x[1]:
                            action_list.append(y)
                elif x[0] == 'runner':
                    runner_list.append(x[1])
                else:
                    other_list.append(x[1])
            for x in inning.get('top').iteritems():
                if x[0] == 'atbat':
                    bat_list.append(x[1])
                elif x[0] == 'action':
                    if type(x[1]) is dict:
                        action_list.append(x[1])
                    else:
                        for y in x[1]:
                            action_list.append(y)
                elif x[0] == 'runner':
                    runner_list.append(x[1])
                else:
                    other_list.append(x[1])

        for inning in bat_list:
            for bat in inning:
                batter = bat.get('@batter')
                b_height = float(bat.get('@b_height').replace('-','.'))
                bat_num = bat.get('@num')
                p_throws = bat.get('@p_throws')
                pitcher = bat.get('@pitcher')
                bat_id = bat.get('@event_num')
                
                extra_dict = {'@batter':batter,
                              '@b_height': b_height,
                              '@bat_num' : bat_num,
                              '@p_throws' : p_throws,
                              '@pitcher' : pitcher,
                              '@bat_id' : bat_id}
                
                if isinstance(bat.get('pitch'),dict):
                    values = bat.get('pitch')
                    values.update(extra_dict)
                    pitch_list.append(values)
                if isinstance(bat.get('pitch'),list):
                    for a_pitch in bat.get('pitch'):
                        values = a_pitch
                        values.update(extra_dict)
                        pitch_list.append(values)
                if isinstance(bat.get('runner'),dict):
                    runner_list.append(bat.get('runner'))
                if isinstance(bat.get('runner'),list):
                    for a_runner in bat.get('runner',{}):
                        runner_list.append(a_runner)

        df_pitch = pd.DataFrame(pitch_list)
        df_action = pd.DataFrame(action_list)
        df_runner = pd.DataFrame(runner_list)

        df_pitch = df_pitch.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        df_pitch['@tfs_zulu'] = pd.to_datetime(df_pitch['@tfs_zulu'])

        df_action = df_action.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        df_action['@tfs_zulu'] = pd.to_datetime(df_action['@tfs_zulu'])

        df_runner = df_runner.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        df_bat = None
        for x in bat_list:
            if df_bat is None:
                df_bat = pd.DataFrame(x)
            else:
                df_bat = df_bat.append(pd.DataFrame(x))

        df_bat.reset_index(inplace=True)
        df_bat = df_bat.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        df_score = pd.concat([df_bat[['@event_num','@away_team_runs','@home_team_runs']], df_pitch[['@event_num']], df_action[['@event_num','@away_team_runs','@home_team_runs']]])
        df_score = df_score.sort_values('@event_num').fillna(method='ffill').fillna(0.0)
        df_pitch = pd.merge(df_pitch, df_score, how='left')

        #Add game_id
        df_pitch['game_id'] = game_id
        df_action['game_id'] = game_id
        df_runner['game_id'] = game_id
        df_bat['game_id'] = game_id    

        df_pitch.columns = ['ax','ay','az','b_height','bat_id','bat_num','batter','break_angle',
        'break_length','break_y','cc','des','des_es','end_speed','event_num','id','mt',
        'nasty','on_1b','on_2b','on_3b','p_throws','pfx_x','pfx_z','pitch_type',
        'pitcher','play_guid','px','pz','spin_dir','spin_rate','start_speed','sv_id',
        'sz_bot','sz_top','tfs','tfs_zulu','type','type_confidence','vx0','vy0','vz0',
        'x','x0','y','y0','z0','zone','away_team_runs','home_team_runs', 'game_id'] 
        df_action.columns = ['away_team_runs','b','des','des_es','event','event_es','event_num',
        'home_team_runs','o','pitch','play_guid','player','s','score','tfs','tfs_zulu','game_id']
        df_runner.columns = ['earned','end','event','event_num','id','rbi','score','start',
        'game_id']
        df_bat.columns = ['index', 'away_team_runs', 'b', 'b_height', 'batter', 'des', 'des_es',
        'event', 'event_es', 'event_num', 'home_team_runs', 'num', 'o', 'p_throws', 'pitcher',
        'play_guid', 's', 'score', 'stand', 'start_tfs', 'start_tfs_zulu', 'pitch', 'po',
        'runner','game_id']

        return df_pitch, df_action, df_runner

    def build_player_df(self, game_path, game_id):
        player_path = game_path + '/' + 'players.xml'

        player_json = xml_path_to_json(player_path)

        game_json = xml_path_to_json(player_path)['game']

        ## [u'umpires', u'@date', u'team', u'@venue']
        game_id = game_id
        game_date = game_json['@date']
        game_venue = game_json['@venue']

        ##umpires
        ##umpire
        ##[u'@position', u'@id', u'@name', u'@last', u'@first']
        umpire_json = game_json['umpires']['umpire']
        umpire_df = pd.DataFrame(umpire_json)
        umpire_df['game_id'] = game_id
        umpire_df['game_date'] = game_date
        umpire_df['game_venue'] = game_venue

        ##team
        ##list of 2
        ##[u'@name', u'coach', u'@id', u'@type', u'player']

        ##coach list
        ##[u'@position', u'@id', u'@first', u'@last', u'@num']

        ##player list
        ##[u'@status', u'@last', u'@rbi', u'@wins', u'@id', u'@losses', 
        ##u'@avg', u'@parent_team_abbrev', u'@era', u'@parent_team_id', 
        ##u'@num', u'@bats', u'@team_abbrev', u'@hr', u'@position', 
        ##u'@rl', u'@first', u'@team_id', u'@boxname']
        player_list = []
        coach_list = []

        for team in game_json['team']:
            team_name = team['@name']
            team_id = team['@id']
            travel_type = team['@type']
            
            for player in team['player']:
                player['team_name'] =  team_name
                player['team_id'] =  team_id
                player['travel_type'] =  travel_type
                player['game_id'] = game_id
                player['game_date'] = game_date
                player['game_venue'] = game_venue
                player_list.append(player)

            for coach in team['coach']:
                coach['team_name'] =  team_name
                coach['team_id'] =  team_id
                coach['travel_type'] =  travel_type
                coach['game_id'] = game_id
                coach['game_date'] = game_date
                coach['game_venue'] = game_venue
                coach_list.append(coach)

        player_df = pd.DataFrame(player_list)
        coach_df = pd.DataFrame(coach_list)


        player_df.columns = ['avg', 'bat_order', 'bats', 'boxname', 'current_position',
        'era', 'first', 'game_position', 'hr', 'id', 'last', 'losses', 'num',
        'parent_team_abbrev', 'parent_team_id', 'position', 'rbi', 'rl', 'status',
        'team_abbrev', 'team_id', 'wins', 'game_date', 'game_id', 'game_venue',
        'team_id', 'team_name', 'travel_type']
        coach_df.columns = ['first','id','last','num','position','game_date','game_id',
        'game_venue','team_id','team_name','travel_type']
        umpire_df.columns = ['first','id','last','name','position','game_id','game_date',
        'game_venue']


        return player_df, coach_df, umpire_df

    def create_data_frames(self):
        #Test Snippet
        # /Volumes/LACIE SHARE/mlb_data/year_2015/month_03/day_07/gid_2015_03_07_slnmlb_wasmlb_1 : gid_2015_03_07_slnmlb_wasmlb_1
        game_id = 'gid_2015_04_04_anamlb_lanmlb_1'
        path = self.game_paths[game_id]

        df_batter = self.build_batter_df(path, game_id)
        df_pitcher = self.build_pitcher_df(path, game_id)
        df_pitch, df_action, df_runner = self.build_inning_df(path, game_id)
        player_df, coach_df, umpire_df = self.build_player_df(path, game_id)

        return df_pitch
        #return df_batter, df_pitcher, df_pitch, df_pitch, df_action, df_runner, df_bat, player_df, coach_df, umpire_df



def xml_path_to_json(path):
    '''
    Turn XML into JSON
    '''
    with open(path) as fd:
        my_json = json.loads(json.dumps(xmltodict.parse(fd.read())))

    return my_json

if __name__ == '__main__':
    path_to_year = sys.argv[1]
    Extractor = MLBExtractor(path_to_year)
    test = Extractor.create_data_frames()
    import pdb; pdb.set_trace()
    print test.head()