import xmltodict
import pandas as pd
from datetime import datetime
import ast

def parse_user_response(content):
    user_content = xmltodict.parse(content)
    return user_content["fantasy_content"]["users"]["user"]["guid"]

def parse_leagues_response(content):
    leagues_content = xmltodict.parse(content)
    games = leagues_content["fantasy_content"]["users"]["user"]["games"]["game"]
    league_dicts = []
    for game in games:
        if  "leagues" in game and game["leagues"] != None:
            game_leagues = game["leagues"]["league"]
            if not isinstance(game_leagues, list):
                game_leagues = [game_leagues]
            for league in game_leagues:
                if league["draft_status"] == "postdraft":
                    league_dict = {
                        "league_key": league["league_key"],
                        "name": league["name"],
                        "game_code": league["game_code"],
                        "season": league["season"],
                        "num_teams": league["num_teams"],
                        "is_finished": "is_finished" in league and league["is_finished"] == "1" 
                    }
                    league_dicts.append(league_dict)
    return pd.DataFrame(league_dicts,
                        columns=['league_key', 'name', 'game_code','season','num_teams', 'is_finished'])

def parse_league_settings_response_to_stat_map(content):
    league_settings_content = xmltodict.parse(content)
    stat_map = {}
    for stat in league_settings_content["fantasy_content"]["leagues"]["league"]["settings"]["stat_categories"]["stats"]["stat"]:
        stat_map[stat['stat_id']] = stat['name']
    return stat_map

def parse_teams_response(content):
    teams_content = xmltodict.parse(content)
    league = teams_content["fantasy_content"]["leagues"]["league"]
    start_date = datetime.strptime(league["start_date"], '%Y-%m-%d')
    start_week = int(league["start_week"])
    # Use current_week if mid-season, end_week + 1 if end of season
    end_week = int(league["current_week"])
    if end_week == int(league["end_week"]):
        end_week += 1
    season = league["season"]
    league_name = league["name"]
    team_dicts = []
    for team in league["standings"]["teams"]["team"]:
        manager = team["managers"]["manager"]
        if isinstance(manager, list):
            manager = manager[0]
        team_dict = {
            "name": team["name"],
            "team_key": team["team_key"],
            "number_of_moves": int(team["number_of_moves"]),
            "number_of_trades": int(team["number_of_trades"]),
            "clinched_playoffs": "clinched_playoffs" in team and team["clinched_playoffs"] == "1",
            "manager_name": manager["nickname"],
            "division_id": team["division_id"] if 'division_id' in team else None,
            "draft_grade": team["draft_grade"] if "draft_grade" in team else None,
            "rank": int(team["team_standings"]["rank"]),
            "points_for": float(team["team_standings"]["points_for"]),
            "points_against": float(team["team_standings"]["points_against"]),
            "wins": int(team["team_standings"]["outcome_totals"]["wins"]),
            "losses": int(team["team_standings"]["outcome_totals"]["losses"]),
        }
        team_dicts.append(team_dict)
    return start_date, start_week, end_week, season, league_name, pd.DataFrame(team_dicts)

def get_nickname(teams_df, team_key):
    return teams_df[teams_df["team_key"] == team_key]["manager_name"].values[0]

def parse_roster_content_list(roster_content_list, teams_data):
    player_dicts = []
    for roster_content in roster_content_list:
        for team in roster_content["fantasy_content"]["teams"]["team"]:
            team_key = team["team_key"]
            for player in team["roster"]["players"]["player"]:
                player_dict = {
                    "player_key": player["player_key"],
                    "name": player["name"]["full"],
                    "position": player["primary_position"],
                    "week": int(player["selected_position"]["week"]),
                    "started": player["selected_position"]['position'] == player["primary_position"] or player["selected_position"]['is_flex'] == '1',
                    "team_key": team_key,
                    "manager_name": get_nickname(teams_data, team_key)
                }
                player_dicts.append(player_dict)
    return pd.DataFrame(player_dicts)

def parse_player_stats_content_list(player_stats_contents, stat_map):
    player_stats_dicts = []
    for player_stats_content in player_stats_contents:
        for player_stats in player_stats_content["fantasy_content"]["leagues"]["league"]["players"]["player"]:
            player_stats_dict = {
                'player_key': player_stats["player_key"],
                'week': int(player_stats["player_stats"]["week"]),
                'points': round(float(player_stats["player_points"]["total"]), 2),
                'name': player_stats["name"]["full"]
            }
            for stat in player_stats["player_stats"]["stats"]["stat"]:
                stat_name = stat_map[stat['stat_id']]
                stat_value = ast.literal_eval(stat['value'])
                player_stats_dict[stat_name] = stat_value
            player_stats_dicts.append(player_stats_dict)
    return pd.DataFrame(player_stats_dicts)

def parse_transactions_response(content, teams_data, start_date):
    transactions_content = xmltodict.parse(content)
    return parse_trades(transactions_content, teams_data, start_date), get_add_drops(transactions_content, teams_data, start_date)

def parse_trades(transactions_content, teams_data, start_date):
    trades = [trade for trade in transactions_content["fantasy_content"]["leagues"]["league"]["transactions"]["transaction"] if trade["type"] == "trade"]
    trade_dicts = []
    for trade in trades:
        trader_player_keys_received = []
        tradee_player_keys_received = []
        trader_player_names_received = []
        tradee_player_names_received = []
        trader_picks_received = []
        tradee_picks_received = []
        trader_team_key = trade["trader_team_key"]
        if "players" in trade:
            players = trade["players"]["player"]
            # single instance if only one player traded    
            if not isinstance(players, list):
                players = [players]
            for player in players:
                name = player["name"]["full"]
                player_key = player["player_key"]
                if player["transaction_data"]["destination_team_key"] == trader_team_key:
                    trader_player_names_received.append(name)
                    trader_player_keys_received.append(player_key)
                else:
                    tradee_player_names_received.append(name)
                    tradee_player_keys_received.append(player_key)
        if "picks" in trade:
            for pick in trade["picks"]["pick"]:
                pick_round = int(pick["round"])
                if pick["destination_team_key"] == trader_team_key:
                    trader_picks_received.append(pick_round)
                else:
                    tradee_picks_received.append(pick_round)
        timestamp = int(trade["timestamp"])
        date = datetime.fromtimestamp(timestamp)
        delta = date - start_date
        week_enacted = delta.days // 7
        trade_dict = {
            "trader_team_key": trader_team_key,
            "trader_nickname": get_nickname(teams_data, trader_team_key),
            "tradee_team_key": trade["tradee_team_key"],
            "tradee_nickname": get_nickname(teams_data, trade["tradee_team_key"]),
            "trader_player_keys_received": ', '.join(trader_player_keys_received),
            "trader_player_names_received": ', '.join(trader_player_names_received),
            "tradee_player_keys_received": ', '.join(tradee_player_keys_received),
            "tradee_player_names_received": ', '.join(tradee_player_names_received),
            "trader_picks_received": ', '.join(trader_picks_received),
            "tradee_picks_received": ', '.join(tradee_picks_received),
            "week_enacted": week_enacted,
            "date": date.strftime("%m/%d/%Y, %H:%M:%S")
        }
        trade_dicts.append(trade_dict)
    return pd.DataFrame(trade_dicts)

def get_add_drops(transactions_content, teams_data, start_date):
    add_drops = [transaction for transaction in transactions_content["fantasy_content"]["leagues"]["league"]["transactions"]["transaction"] if transaction["type"] == "add/drop"]
    add_drop_dicts = []

    for add_drop in add_drops:
        player_keys_added = []
        player_keys_dropped = []
        player_names_added = []
        player_names_dropped = []
        manager_team_key = None
        if "players" in add_drop:
            players = add_drop["players"]["player"]
            # single instance if only one player traded    
            if not isinstance(players, list):
                players = [players]
            for player in players:
                name = player["name"]["full"]
                player_key = player["player_key"]
                if player["transaction_data"]["type"] == "add":
                    player_names_added.append(name)
                    player_keys_added.append(player_key)
                    if manager_team_key == None:
                        manager_team_key = player["transaction_data"]["destination_team_key"]
                else:
                    player_names_dropped.append(name)
                    player_keys_dropped.append(player_key)
        timestamp = int(add_drop["timestamp"])
        date = datetime.fromtimestamp(timestamp)
        delta = date - start_date
        week_enacted = delta.days // 7
        add_drop_dict = {
            "manager_team_key": manager_team_key,
            "manager_nickname": get_nickname(teams_data, manager_team_key),
            "player_keys_added": ', '.join(player_keys_added),
            "player_names_added": ', '.join(player_names_added),
            "player_keys_dropped": ', '.join(player_keys_dropped),
            "player_names_dropped": ', '.join(player_names_dropped),
            "week_enacted": week_enacted,
            "date": date.strftime("%m/%d/%Y, %H:%M:%S")
        }
        add_drop_dicts.append(add_drop_dict)
    return pd.DataFrame(add_drop_dicts)

def parse_matchups_response(content, teams_data):
    matchups_content = xmltodict.parse(content)
    matchup_dicts = []
    for team in matchups_content["fantasy_content"]["teams"]["team"]:
        for matchup in team["matchups"]["matchup"]:
            team_1 = matchup["teams"]["team"][0]
            matchup_dict = {
                "week": int(matchup['week']),
                "is_playoffs": matchup["is_playoffs"] == "1",
                "is_consolation": matchup["is_consolation"] == "1",
                "team_1_key": team_1["team_key"],
                "team_1_nickname": get_nickname(teams_data, team_1["team_key"]),
                "team_1_points": float(team_1["team_points"]["total"]),
                "team_1_projected_points": float(team_1["team_projected_points"]["total"])
            }
            if matchup["teams"]["@count"] == "1":
                matchup_dict["is_bye"] = True
            else:
                matchup_dict["is_bye"] = False
                team_2 = matchup["teams"]["team"][1]
                matchup_dict["team_2_key"] = team_2["team_key"]
                matchup_dict["team_2_nickname"] = get_nickname(teams_data, team_2["team_key"])
                matchup_dict["team_2_points"] = team_2["team_points"]["total"]
                matchup_dict["team_2_projected_points"] = team_2["team_projected_points"]["total"]
            matchup_dicts.append(matchup_dict)
    return pd.DataFrame(matchup_dicts)

def merge_player_stats_data(players_df, player_stats_df):
    if len(player_stats_df) > 0 and len(players_df) > 0:
        players_df = pd.merge(players_df, player_stats_df, how='outer', left_on=['player_key','week', 'name'], right_on = ['player_key', 'week', 'name'])
    return players_df.fillna(0)

def get_player_week_key(season, player_key, week):
    return f"{season}_{player_key}_{week}"

def get_season_player_key_week_from_key(key):
    season, player_key, week = key.split("_")
    return season, player_key, week

def get_player_week_keys(players_df, season):
    player_week_keys = []
    # iterate rows of players_df.  Get player_week_key for player_key and week.
    for index, row in players_df.iterrows():
        player_key = row["player_key"]
        week = row["week"]
        player_week_keys.append(get_player_week_key(season, player_key, week))
    return player_week_keys

def parse_draft_results_response(content, teams_data, player_data):
    draft_results_content = xmltodict.parse(content)
    leagues = draft_results_content["fantasy_content"]["leagues"]["league"]
    if not isinstance(leagues, list):
        leagues = [leagues]
    pick_dicts = []
    for league in leagues:
        for pick in league["draft_results"]["draft_result"]:
            pick_dict = {
                "pick": int(pick["pick"]),
                "round": int(pick["round"]),
                "team_key": pick["team_key"],
                "player_key": pick["player_key"],
            }
            pick_dicts.append(pick_dict)
    picks_df = pd.DataFrame(pick_dicts)
    picks_df = picks_df.merge(teams_data[["team_key", "manager_name"]], how="left", on="team_key")
    picks_df = picks_df.merge(player_data[["player_key", "name", "position"]].drop_duplicates(keep="first"), how="left", on="player_key")
    return picks_df.dropna()