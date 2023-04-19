import time
import traceback

import requests


def get_spotify_access_token_data(client_id, client_secret, endpoint):
    print("get_spotify_access_token_data")
    try:
        current_time = time.time()
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'}

        body = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret}

        access_data_response = requests.request(
            "POST",
            url=endpoint,
            headers=headers,
            params=body)

        print("body: ", body)

        if access_data_response.status_code == 200:
            access_data_info = access_data_response.json()
            access_token_expire_time = access_data_info["expires_in"] + current_time
            return access_data_info["access_token"], access_data_info["token_type"], access_token_expire_time, None
        else:
            return None, None, None, \
                f"access_data_response status = {access_data_response.status_code}, " \
                f"access_data_response error = {access_data_response.text}, {traceback.format_exc()}"

    except Exception as error:
        raise error


def get_spotify_artist_data(token_type, access_token, endpoint):
    print("get_spotify_artist_data")
    try:
        payload = {}
        headers = {
            'Authorization': f'{token_type}  {access_token}'}

        artist_data_response = requests.request(
            "GET",
            url=endpoint,
            headers=headers,
            data=payload)

        if artist_data_response.status_code == 200:
            print(f"artist_data_response = {artist_data_response.json()}")
            return artist_data_response.json(), None
        else:
            return None, f"artist_data_response status = {artist_data_response.status_code}, " \
                         f"artist_data_response error = {artist_data_response.text}"
    except Exception as error:
        raise error


def get_spotify_search_data(query, search_type, token_type, access_token, endpoint, offset=None):
    print("get_spotify_search_data", query, type(query))

    url = f"{endpoint}?q={query}&type={search_type}" \
        if offset is None \
        else f"{endpoint}?q={query}&type={search_type}&offset={offset}"

    try:
        payload = ""
        headers = {
            'Authorization': f'{token_type} {access_token}'}
        search_data_response = requests.request(
            "GET",
            url=url,
            headers=headers,
            data=payload)

        if search_data_response.status_code == 200:
            return search_data_response.json(), None
        else:
            return search_data_response.text, search_data_response.status_code

    except Exception as error:
        raise error


def get_spotify_playlist_data(playlist_id, token_type, access_token, endpoint):
    print("get_spotify_playlist_data", playlist_id, type(playlist_id))

    try:
        payload = ""
        headers = {
            'Authorization': f'{token_type} {access_token}'}
        playlist_data_response = requests.request(
            "GET",
            url=f"{endpoint}/{playlist_id}",
            headers=headers,
            data=payload)

        if playlist_data_response.status_code == 200:
            return playlist_data_response.json(), None
        else:
            return None, f"playlist_data_response status = {playlist_data_response.status_code}, " \
                         f"playlist_data_response error = {playlist_data_response.text}"
    except Exception as error:
        raise error


class SpotifyOutOfRangeWarning(Exception):
    def __init__(self, spotify_response_code, spotify_response_message):
        self.error_code = spotify_response_code
        self.message = spotify_response_message


class SpotifyInvalidAccessToken(Exception):
    def __init__(self, spotify_response_code, spotify_response_message):
        self.error_code = spotify_response_code
        self.message = spotify_response_message
