import datetime
import json
import time
import traceback

import requests

from source_spotify.spotify import constants


class SpotifyAPI:
    def __init__(self):
        self.access_token_expire_time = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.token_type = None

    def update_authentication_data(self):
        self.access_token, self.token_type, self.access_token_expire_time, error = \
            SpotifyAPI.get_spotify_access_token_data(
                client_id=self.client_id,
                client_secret=self.client_secret,
                endpoint=constants.SPOTIFY_ACCESS_TOKEN_ENDPOINT
            )
        return error

    def load_config_data(self, config: json):
        self.client_id = config.get('credentials').get("client_id")
        self.client_secret = config.get('credentials').get("client_secret")

    def get_spotify_search_data(self, query, search_type, limit=None, offset=None):

        endpoint = constants.SPOTIFY_SEARCH_ENDPOINT

        url = f"{endpoint}?q={query}&type={search_type}" \
            if (offset is None or limit is None) \
            else f"{endpoint}?q={query}&type={search_type}&limit={limit}&offset={offset}"

        try:
            payload = ""
            headers = {
                'Authorization': f'{self.token_type} {self.access_token}'}
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

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def get_current_timestamp():
        return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
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

    @staticmethod
    def get_search_track_properties_default():
        return {
            "album": {
                "type": "object",
                "properties": {
                    "album_type": {
                        "type": "string",
                        "description": "The type of the album: one of 'album', "
                                       "'single', or 'compilation'."
                    },
                    "available_markets": {
                        "type": "array",
                        "description": "The markets in which the album is available: "
                                       "ISO 3166-1 alpha-2 country codes. Note that "
                                       "an album is considered available in a market "
                                       "when at least 1 of its tracks is available in "
                                       "that market.",
                        "items": {
                            "type": "string"
                        }
                    },
                    "external_urls": {
                        "type": "object",
                        "description": "Known external URLs for this album.",
                        "properties": {
                            "spotify": {
                                "type": "string",
                                "description": "The type of the URL, for example: "
                                               "'spotify' - The Spotify URL for the object."
                            }
                        }
                    },
                    "href": {
                        "type": "string",
                        "description": "A link to the Web API endpoint providing "
                                       "full details of the album."
                    },
                    "id": {
                        "type": "string",
                        "description": "The Spotify ID for the album."
                    },
                    "images": {
                        "type": "array",
                        "description": "The cover art for the album in various sizes, widest first.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "height": {
                                    "type": "integer",
                                    "description": "The image height in pixels. "
                                                   "If unknown: null or not returned."
                                },
                                "url": {
                                    "type": "string",
                                    "description": "The source URL of the image."
                                },
                                "width": {
                                    "type": "integer",
                                    "description": "The image width in pixels. "
                                                   "If unknown: null or not returned."
                                }
                            }
                        }
                    },
                    "name": {
                        "type": "string",
                        "description": "The name of the album."
                    },
                    "type": {
                        "type": "string",
                        "description": "The object type: 'album'."
                    },
                    "uri": {
                        "type": "string",
                        "description": "The Spotify URI for the album."
                    }
                }
            },
            "artists": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "external_urls": {
                            "type": "object",
                            "description": "Known external URLs for this artist.",
                            "spotify": {
                                "type": "string"
                            }
                        },
                        "href": {
                            "type": "string",
                            "description": "A link to the Web API endpoint providing "
                                           "full details of the artist."
                        },
                        "id": {
                            "type": "string",
                            "description": "The Spotify ID for the artist."
                        },
                        "name": {
                            "type": "string",
                            "description": "The name of the artist."
                        },
                        "type": {
                            "type": "string",
                            "description": "The object type: 'artist'"
                        },
                        "uri": {
                            "type": "string",
                            "description": "The Spotify URI for the artist."
                        }
                    }
                }
            },
            "available_markets": {
                "type": "array",
                "description": "A list of the countries in which the track can be played, "
                               "identified by their ISO 3166-1 alpha-2 code. ",
                "items": {
                    "type": "string"
                }
            },
            "disc_number": {
                "type": "integer",
                "description": "The disc number (usually 1 unless the album consists of "
                               "more than one disc)."
            },
            "duration_ms": {
                "type": "integer",
                "description": "The track length in milliseconds."
            },
            "explicit": {
                "type": "boolean",
                "description": "Whether or not the track has explicit lyrics (true = yes "
                               "it does; false = no it does not OR unknown)."
            },
            "external_ids": {
                "type": "object",
                "description": "Known external IDs for the track.",
                "properties": {
                    "isrc": {
                        "type": "string",
                        "description": "The identifier type, for example: 'isrc' - "
                                       "International Standard Recording Code, 'ean' - "
                                       "International Article Number, 'upc' - Universal "
                                       "Product Code"
                    }
                }
            },
            "external_urls": {
                "type": "object",
                "description": "Known external URLs for this track.",
                "properties": {
                    "spotify": {
                        "type": "string",
                        "description": "The type of the URL, for example: 'spotify' - "
                                       "The Spotify URL for the object."
                    }
                }
            },
            "href": {
                "type": "string",
                "description": "A link to the Web API endpoint providing "
                               "full details of the track."
            },
            "id": {
                "type": "string",
                "description": "The Spotify ID for the track."
            },
            "is_local": {
                "type": "boolean"
            },
            "name": {
                "type": "string",
                "description": "The name of the track."
            },
            "popularity": {
                "type": "integer"
            },
            "preview_url": {
                "type": "string",
                "description": "A URL to a 30 second preview (MP3 format) of the track."
            },
            "track_number": {
                "type": "integer",
                "description": "The number of the track. If an album has several discs, "
                               "the track number is the number on the specified disc."
            },
            "type": {
                "type": "string",
                "description": "The object type: 'track'."
            },
            "uri": {
                "type": "string",
                "description": "The Spotify URI for the track."
            }
        }
