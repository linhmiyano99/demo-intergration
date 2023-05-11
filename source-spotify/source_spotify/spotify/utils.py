import datetime
import time
import traceback

import requests


def get_spotify_access_token_data(client_id, client_secret, endpoint):
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
            return artist_data_response.json(), None
        else:
            return None, f"artist_data_response status = {artist_data_response.status_code}, " \
                         f"artist_data_response error = {artist_data_response.text}"
    except Exception as error:
        raise error


def get_spotify_search_data(query, search_type, token_type, access_token, endpoint, limit=None, offset=None):
    url = f"{endpoint}?q={query}&type={search_type}" \
        if (offset is None or limit is None) \
        else f"{endpoint}?q={query}&type={search_type}&limit={limit}&offset={offset}"

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
            if search_data_response.json().get("error") is not None:
                return search_data_response.json().get("error").get("message"), search_data_response.status_code
            return search_data_response.json(), search_data_response.status_code

    except Exception as error:
        raise error


def get_spotify_playlist_data(playlist_id, token_type, access_token, endpoint):
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


def backoff(retries=2, delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            is_success = False
            retrial = 0
            error = None
            while retrial < retries and not is_success:
                retrial += 1
                try:
                    return func(*args, **kwargs)
                except SpotifyOutOfRangeWarning or SpotifyInvalidAccessToken as e:
                    raise e
                except Exception as e:
                    error = e
                    time.sleep(delay)
            raise error

        return wrapper

    return decorator


def get_current_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def get_event_from_track(track, namespace, timestamp):
    event = {
        'eventType': 'SaveEntity',
        'timeStamp': timestamp,
        'target': {
            'itemType': 'track',
            'itemId': track['id'],
            'properties': {
                'album': track.get('album'),
                'artists': track.get('artists'),
                'available_markets': track.get('available_markets'),
                'disc_number': track.get('disc_number'),
                'duration_ms': track.get('duration_ms'),
                'explicit': track.get('explicit'),
                'external_ids': track.get('external_ids'),
                'external_urls': track.get('external_urls'),
                'href': track.get('href'),
                'id': track.get('id'),
                'is_local': track.get('is_local'),
                'name': track.get('name'),
                'popularity': track.get('popularity'),
                'preview_url': track.get('preview_url'),
                'track_number': track.get('track_number'),
                'type': track.get('type'),
                'url': track.get('url'),
            }
        }
    }
    _id = event['target']['itemId']
    _type = event['target']['itemType']
    event['itemId'] = f"{namespace}.{event['eventType']}.{_type}_{_id}"
    return event
