import json
import time
import traceback

import requests
from utils import constants


def get_spotify_access_token_data(client_id, client_secret):
    client_id = "1246fcf55a9745a5940b3e96c9900f8c"
    client_secret = "49f262a12a3b49d290579957e9ae22ea"
    try:
        current_time = time.time()
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        body = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
        access_data_response = requests.request(
            "POST",
            url=f"{constants.SPOTIFY_URL}{constants.SPOTIFY_ACCESS_TOKEN_ENDPOINT}",
            headers=headers,
            params=body
        )

        print("body: ", body)
        if access_data_response.status_code == 200:
            access_data_info = access_data_response.json()
            access_token_expire_time = access_data_info["expires_in"] + current_time
            return access_data_info["access_token"], access_data_info["token_type"], access_token_expire_time, None
        else:
            return None, None, None, f"access_data_response status = {access_data_response.status_code}, " \
                  f"access_data_response error = {access_data_response.text}, {traceback.format_exc()}"
    except Exception as error:
        raise error


def get_spotify_artist_data(token_type, access_token):
    token_type = "Bearer"
    access_token = "BQB1hvejRdpapXy9ejNM3xjkBIkHebIFHykDddx6Fn6nMCn-LfVDqaK-8p5RsgZbvP7Z7mWXZnsMUak4vWfSeemkksOWOPRVTkDTMkxWhC9HCC0hR7Kj"
    try:
        payload = {}
        headers = {
            'Authorization': f'{token_type}  {access_token}"'
        }

        artist_data_response = requests.request(
            "GET",
            url=f"{constants.SPOTIFY_API_URL}{constants.SPOTIFY_ARTIST_ENDPOINT}",
            headers=headers,
            data=payload
        )
        if artist_data_response.status_code == 200:
            return artist_data_response.json(), None
        else:
            return None, f"artist_data_response status = {artist_data_response.status_code}, " \
                  f"artist_data_response error = {artist_data_response.text}"
    except Exception as error:
        raise error
