from flask import (
    Blueprint, request
)
from flaskr.yahoo_client import YahooClient
from flaskr.db_client import DBClient
from flaskr.db_processor import merge_leagues_df_db

bp = Blueprint('leagues', __name__)

@bp.route('/leagues', methods=['GET'])
def get_leagues():
    # get the parameter "access_token" sent by the user  
    access_token = request.args.get('access_token')
    yahoo_client = YahooClient(access_token)
    db_client = DBClient()

    user_id = yahoo_client.get_user_id()

    db_leagues = db_client.get_leagues(user_id)
    leagues_df = yahoo_client.get_leagues()
    leagues_df = merge_leagues_df_db(leagues_df, db_leagues)

    return leagues_df.to_json(orient='records')
    
@bp.route('/leagues_test', methods=['GET'])
def get_leagues_test():
    return [{"league_key":"222.l.498135","name":"Johnny Bravo","game_code":"nfl","season":"2009","num_teams":"12","is_finished":True},{"league_key":"242.l.54796","name":"thug life","game_code":"nfl","season":"2010","num_teams":"12","is_finished":True},{"league_key":"257.l.37493","name":"Think with your dipstick","game_code":"nfl","season":"2011","num_teams":"10","is_finished":True},{"league_key":"273.l.11763","name":"RIght in the jejunum","game_code":"nfl","season":"2012","num_teams":"10","is_finished":True},{"league_key":"314.l.72103","name":"Homies in my backpack","game_code":"nfl","season":"2013","num_teams":"10","is_finished":True},{"league_key":"331.l.35354","name":"Chyea In Da Buildin","game_code":"nfl","season":"2014","num_teams":"10","is_finished":True},{"league_key":"348.l.88412","name":"Chyea In Da Buildin","game_code":"nfl","season":"2015","num_teams":"10","is_finished":True},{"league_key":"359.l.62508","name":"Chyea In Da Buildin","game_code":"nfl","season":"2016","num_teams":"10","is_finished":True},{"league_key":"371.l.120246","name":"Chyea In Da Buildin","game_code":"nfl","season":"2017","num_teams":"10","is_finished":True},{"league_key":"380.l.709344","name":"Chyea In Da Buildin","game_code":"nfl","season":"2018","num_teams":"10","is_finished":True},{"league_key":"390.l.99174","name":"Chyea In Da Buildin","game_code":"nfl","season":"2019","num_teams":"10","is_finished":True},{"league_key":"390.l.1048736","name":"The One Where Jon Wins Money","game_code":"nfl","season":"2019","num_teams":"16","is_finished":True},{"league_key":"390.l.1265419","name":"Y! Fantasy Sucks","game_code":"nfl","season":"2019","num_teams":"16","is_finished":True},{"league_key":"399.l.43040","name":"Chyea In Da Buildin","game_code":"nfl","season":"2020","num_teams":"10","is_finished":True},{"league_key":"399.l.1104944","name":"smoky seattle FFFF","game_code":"nfl","season":"2020","num_teams":"10","is_finished":True, "export_url": "https://docs.google.com/spreadsheets/d/1icaGeSNDP7-ctB6yIP0IXAea9dmdk2PnPzUf2KnsS2Q/edit#gid=0" },{"league_key":"406.l.27951","name":"Chyea In Da Buildin","game_code":"nfl","season":"2021","num_teams":"10","is_finished":True},{"league_key":"414.l.49991","name":"Chyea In Da Buildin","game_code":"nfl","season":"2022","num_teams":"10","is_finished":False}]