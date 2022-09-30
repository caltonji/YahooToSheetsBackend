from flask import (
    Blueprint, request
)
from flaskr.yahoo_client import YahooClient
from flaskr.sheets_client import SheetsClient
from flaskr.db_client import DBClient
from flaskr.yahoo_response_parser import merge_player_stats_data, get_player_week_keys


bp = Blueprint('export', __name__)

@bp.route('/export', methods=['POST'])
def post_export():
    # get the parameter "access_token" and "league_key" sent by the user
    if "access_token" not in request.json:
        return "access_token is missing", 400
    if "league_key" not in request.json:
        return "league_key is missing", 400
    access_token = request.json["access_token"]
    league_key = request.json["league_key"]

    # create outbound connections
    yahoo_client = YahooClient(access_token)
    db_client = DBClient()
    sheets_client = SheetsClient()
    print("Created the clients")

    # get user_id from yahoo
    user_id = yahoo_client.get_user_id()
    print("Validated logged in user")

    # Get teams data and upload to google sheets
    start_date, start_week, end_week, season, name, teams_data = yahoo_client.get_teams_data(league_key)

    # Create a Google Sheet
    sh = sheets_client.create_sheet(name + " " + season)
    print("Created a new google sheet")

    sheets_client.upload_df(sh, teams_data, "managers")
    print("Uploaded manager data")

    # Get player data and upload to google sheets
    player_data = yahoo_client.get_players_data(start_week, end_week, teams_data)
    stat_map = yahoo_client.get_stat_map(league_key)
    player_week_keys = get_player_week_keys(player_data, season)
    print("Fetched roster data")

    # First get player stats from DB
    player_stats_data_from_db = db_client.get_player_data(player_week_keys, season)
    player_data_with_stats = merge_player_stats_data(player_data, player_stats_data_from_db)
    # get the remaining player_stats_data from yahoo
    # Get the player_keys that are in the list player_keys but not in player_stats_data_from_db["player_key"]
    player_week_keys_to_get_from_yahoo = player_week_keys
    if len(player_stats_data_from_db) > 0:
        player_week_keys_from_db = get_player_week_keys(player_stats_data_from_db, season)
        player_week_keys_to_get_from_yahoo = list(set(player_week_keys) - set(player_week_keys_from_db))
    print(f"Found {len(player_stats_data_from_db)} in DB.  Fetching {len(player_week_keys_to_get_from_yahoo)} player week stats from Yahoo")
    player_stats_data_from_yahoo = yahoo_client.get_player_stats_data(league_key, player_week_keys_to_get_from_yahoo, stat_map)
    player_data_with_stats = merge_player_stats_data(player_data_with_stats, player_stats_data_from_yahoo)
    print("Fetched player stats data from yahoo")
    db_client.save_player_data(player_stats_data_from_yahoo, season)
    print("Saved player data in DB")


    sheets_client.upload_df(sh, player_data_with_stats, "players")
    print("uploaded player data in sheets")

    # Get transactions data and upload to google sheets
    trades_data, add_drops_data = yahoo_client.get_transactions_data(league_key, teams_data, start_date)
    sheets_client.upload_df(sh, trades_data, "trades")
    sheets_client.upload_df(sh, add_drops_data, "add drops")
    print("Uploaded transactions data")
    
    # Get matchups data and upload to google sheets
    matchups_data = yahoo_client.get_matchups_data(teams_data)
    sheets_client.upload_df(sh, matchups_data, "matchups")
    print("Uploaded matchups data")

    # Get draft results
    draft_results_data = yahoo_client.get_draft_results_data(league_key, teams_data, player_data_with_stats)
    sheets_client.upload_df(sh, draft_results_data, "draft results")
    print("Uploaded draft results")

    sheets_client.copy_example_ws(sh)
    print("Copied the example worksheet")

    # Share the google sheet to the user
    sheets_client.share_sheet(sh)
    print("Shared the sheet with the user")

    db_client.upsert_league(user_id, league_key, sh.url)

    # return the url of the google sheet
    return sh.url
    
@bp.route('/export_test', methods=['POST'])
def post_export_test():
    # wait for 1 second than return success
    import time
    time.sleep(1)
    return "success"

