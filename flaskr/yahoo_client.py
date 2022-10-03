import os
import requests
import xmltodict
import pandas as pd
from flaskr.yahoo_response_parser import parse_leagues_response, parse_league_settings_response_to_stat_map, parse_teams_response, parse_roster_content_list, parse_player_stats_response_list, parse_transactions_response, parse_matchups_response, parse_user_response, get_season_player_key_week_from_key, parse_draft_results_response

class YahooClient:
    def __init__(self, access_token):
        self.access_token = access_token
        self.headers = {
            'Authorization': 'Bearer ' + self.access_token
        }
        
    def get_user_id(self):
        response = requests.get("https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1", headers=self.headers)
        response.raise_for_status()

        # parse response
        return parse_user_response(response.text)

    def get_leagues(self):
        response = requests.get("https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games/leagues", headers=self.headers)
        response.raise_for_status()

        # response data is XML, convert to pandas
        return parse_leagues_response(response.text)

    def get_stat_map(self, league_key):
        uri = f"https://fantasysports.yahooapis.com/fantasy/v2/leagues;league_keys={league_key}/settings"   
        response = requests.get(uri, headers=self.headers)
        response.raise_for_status()

        # response data is XML, convert to stat map
        return parse_league_settings_response_to_stat_map(response.text)

    def get_teams_data(self, league_key):
        uri = f"https://fantasysports.yahooapis.com/fantasy/v2/leagues;league_keys={league_key}/standings"
        response = requests.get(uri, headers=self.headers)
        response.raise_for_status()

        # response data is XML, convert to pandas
        return parse_teams_response(response.text)


    def get_players_data(self, start_week, end_week, teams_data):
        team_keys = ",".join(teams_data["team_key"])
        baseUri = f"https://fantasysports.yahooapis.com/fantasy/v2/teams;team_keys={team_keys}/roster"
        roster_content_list = []
        for i in range(start_week, end_week):
            uri = baseUri + ";week=" + str(i)
            response = requests.get(uri, headers=self.headers)
            response.raise_for_status()

            roster_content = xmltodict.parse(response.text)
            roster_content_list.append(roster_content)
        

        # response data is XML, convert to pandas
        return parse_roster_content_list(roster_content_list, teams_data)

    def get_player_stats_data(self, league_key, player_week_keys, stat_map):
        if len(player_week_keys) == 0:
            return pd.DataFrame()
        # convert player_week_keys into batches of 25, organized by week
        # easier to group by with pandas
        player_week_dicts = []
        for player_week_key in player_week_keys:
            season, player_key, week = get_season_player_key_week_from_key(player_week_key)
            player_week_dicts.append({
                "season": season,
                "player_key": player_key,
                "week": week
            })
        fetch_df = pd.DataFrame(player_week_dicts)
        # break up player_keys into weeks, and each week into chunks of 25
        player_week_keys_by_week = fetch_df.groupby("week")["player_key"].apply(list).to_dict()
        for week in player_week_keys_by_week:
            player_week_keys_by_week[week] = [player_week_keys_by_week[week][i:i+25] for i in range(0, len(player_week_keys_by_week[week]), 25)]
        
        player_stats_texts = []
        for week in player_week_keys_by_week:
            for batch in player_week_keys_by_week[week]:
                player_keys_subset_str = ",".join(batch)
                uri = f"https://fantasysports.yahooapis.com/fantasy/v2/leagues;league_keys={league_key}/players;player_keys={player_keys_subset_str}/stats;type=week;week={week}"   
                response = requests.get(uri, headers=self.headers)
                response.raise_for_status()

                player_stats_texts.append(response.text)
        
        # response data is XML, convert to pandas
        try:
            return parse_player_stats_response_list(player_stats_texts, stat_map)
        except:
            print("Error parsing player stats content list")
            print(player_stats_texts)
            raise

    def get_transactions_data(self, league_key, teams_data, start_date):
        uri = f"https://fantasysports.yahooapis.com/fantasy/v2/leagues;league_keys={league_key};out=transactions"
        response = requests.get(uri, headers=self.headers)
        response.raise_for_status()
        
        # response data is XML, convert to pandas
        return parse_transactions_response(response.text, teams_data, start_date)

    def get_matchups_data(self, teams_data):
        team_keys = ",".join(teams_data["team_key"])
        uri = f"https://fantasysports.yahooapis.com/fantasy/v2/teams;team_keys={team_keys}/matchups"
        response = requests.get(uri, headers=self.headers)
        response.raise_for_status()

        # response data is XML, convert to pandas
        return parse_matchups_response(response.text, teams_data)
        

    def get_draft_results_data(self, league_key, teams_data, player_data):
        uri = f"https://fantasysports.yahooapis.com/fantasy/v2/leagues;league_keys={league_key}/draftresults"   
        response = requests.get(uri, headers=self.headers)
        response.raise_for_status()

        return parse_draft_results_response(response.text, teams_data, player_data)