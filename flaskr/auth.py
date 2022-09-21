import os
from flask import (
    Blueprint, request
)
import requests

bp = Blueprint('auth', __name__)

@bp.route('/auth', methods=['GET'])
def get_auth():
    # get the parameter "yahoo_code" sent by the user  
    if "yahoo_code" in request.args:
        yahoo_code = request.args.get('yahoo_code')
        data =  {
            'grant_type': 'authorization_code',
            'code': yahoo_code,
            'redirect_uri': os.environ["yahoo_redirect_uri"],
            'client_id': os.environ["yahoo_client_id"],
            'client_secret': os.environ["yahoo_client_secret"]
        }
        response = requests.post("https://api.login.yahoo.com/oauth2/get_token", data=data)
        response.raise_for_status()
        # response data has fields access_token, refresh_token and expires_in
        return response.json()
    elif "refresh_token" in request.args:
        refresh_token = request.args.get('refresh_token')
        data =  {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'redirect_uri': os.environ["yahoo_redirect_uri"],
            'client_id': os.environ["yahoo_client_id"],
            'client_secret': os.environ["yahoo_client_secret"]
        }
        response = requests.post("https://api.login.yahoo.com/oauth2/get_token", data=data)
        response.raise_for_status()
        # response data has fields access_token, refresh_token and expires_in
        return response.json()
    else:
        return {}