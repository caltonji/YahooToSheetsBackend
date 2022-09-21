import os
import requests
import xmltodict
from flaskr.yahoo_response_parser import parse_leagues_response, parse_league_settings_response_to_stat_map, parse_teams_response, parse_roster_content_list, parse_player_stats_content_list, parse_transactions_response, parse_matchups_response, parse_user_response

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

    def get_player_stats_data(self, league_key, player_keys, start_week, end_week, stat_map):
        player_stats_contents = []
        if len(player_keys) > 0:
            for week in range(start_week, end_week):
                # max page size is 25
                print(f"downloading week {week}")
                for i in range(0,len(player_keys),25):
                    player_keys_subset_str = ",".join(player_keys[i:i+25])
                    uri = f"https://fantasysports.yahooapis.com/fantasy/v2/leagues;league_keys={league_key}/players;player_keys={player_keys_subset_str}/stats;type=week;week={week}"   
                    response = requests.get(uri, headers=self.headers)
                    response.raise_for_status()

                    player_stats_content = xmltodict.parse(response.text)
                    player_stats_contents.append(player_stats_content)
        
        # response data is XML, convert to pandas
        return parse_player_stats_content_list(player_stats_contents, stat_map)

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