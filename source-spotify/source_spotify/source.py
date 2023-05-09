#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
import time
import traceback
from datetime import datetime
from typing import Dict, Generator

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import Source
from airbyte_protocol.models import SyncMode, ConfiguredAirbyteStream

from .spotify.utils import SpotifyInvalidAccessToken, SpotifyOutOfRangeWarning, backoff
from .spotify import utils, constants


class SourceSpotify(Source):
    def __init__(self):
        self.access_token = None
        self.token_type = None
        self.access_token_expire_time = None
        self.client_id = None
        self.client_secret = None
        self.offset = 0
        self.limit_items_in_call = None
        self.size_record_countdown = constants.DEFAULT_SIZE_RECORD

    def load_config_data(self, config: json):
        self.client_id = config.get('credentials').get("client_id")
        self.client_secret = config.get('credentials').get("client_secret")

    def update_authentication_data(self):
        self.access_token, self.token_type, self.access_token_expire_time, error = \
            utils.get_spotify_access_token_data(
                client_id=self.client_id,
                client_secret=self.client_secret,
                endpoint=constants.SPOTIFY_ACCESS_TOKEN_ENDPOINT
            )
        return error

    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        self.load_config_data(config=config)
        try:
            # Not Implemented
            error = self.update_authentication_data()
            if error is not None:
                error = f"error: {error}"
                print(error)
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message=f"An exception occurred: {error} trace = {traceback.format_exc()}")

            _, error = utils.get_spotify_search_data(
                query="a",
                search_type="track",
                limit=1,
                offset=0,
                token_type=self.token_type,
                access_token=self.access_token,
                endpoint=constants.SPOTIFY_SEARCH_ENDPOINT)

            if error is not None:
                error = f"error: {error}"
                print(error)
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message=f"An exception occurred: {error} trace = {traceback.format_exc()}")

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"An exception occurred: {str(e)} trace = {traceback.format_exc()}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []

        stream_name = "spotify.search.track"
        json_schema = {  # Example
            "$schema": "http://json-schema.org/draft-03/schema#",
            "type": "object",
            "properties": {
                "album": {
                    "type": "object",
                    "properties": {
                        "album_type": {
                            "type": "string",
                            "description": "The type of the album: one of 'album', 'single', or 'compilation'."
                        },
                        "available_markets": {
                            "type": "array",
                            "description": "The markets in which the album is available: ISO 3166-1 alpha-2 country codes. Note that an album is considered available in a market when at least 1 of its tracks is available in that market.",
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
                                    "description": "The type of the URL, for example: 'spotify' - The Spotify URL for the object."
                                }
                            }
                        },
                        "href": {
                            "type": "string",
                            "description": "A link to the Web API endpoint providing full details of the album."
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
                                        "description": "The image height in pixels. If unknown: null or not returned."
                                    },
                                    "url": {
                                        "type": "string",
                                        "description": "The source URL of the image."
                                    },
                                    "width": {
                                        "type": "integer",
                                        "description": "The image width in pixels. If unknown: null or not returned."
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
                                "description": "A link to the Web API endpoint providing full details of the artist."
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
                    "description": "A list of the countries in which the track can be played, identified by their ISO 3166-1 alpha-2 code. ",
                    "items": {
                        "type": "string"
                    }
                },
                "disc_number": {
                    "type": "integer",
                    "description": "The disc number (usually 1 unless the album consists of more than one disc)."
                },
                "duration_ms": {
                    "type": "integer",
                    "description": "The track length in milliseconds."
                },
                "explicit": {
                    "type": "boolean",
                    "description": "Whether or not the track has explicit lyrics (true = yes it does; false = no it does not OR unknown)."
                },
                "external_ids": {
                    "type": "object",
                    "description": "Known external IDs for the track.",
                    "properties": {
                        "isrc": {
                            "type": "string",
                            "description": "The identifier type, for example: 'isrc' - International Standard Recording Code, 'ean' - International Article Number, 'upc' - Universal Product Code"
                        }
                    }
                },
                "external_urls": {
                    "type": "object",
                    "description": "Known external URLs for this track.",
                    "properties": {
                        "spotify": {
                            "type": "string",
                            "description": "The type of the URL, for example: 'spotify' - The Spotify URL for the object."
                        }
                    }
                },
                "href": {
                    "type": "string",
                    "description": "A link to the Web API endpoint providing full details of the track."
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
                    "description": "The number of the track. If an album has several discs, the track number is the number on the specified disc."
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
        }

        # Not Implemented

        streams.append(
            AirbyteStream(
                name=stream_name,
                json_schema=json_schema,
                supported_sync_modes=[SyncMode.incremental],
                default_cursor_field=["id"]))
        return AirbyteCatalog(streams=streams)

    @backoff(retries=2, delay=1)
    def get_batch_data(
            self, logger: AirbyteLogger, stream_name: str, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        print("get_batch_data is processing")

        search_data_batch, error = utils.get_spotify_search_data(
            query=constants.SPOTIFY_SEARCH_KEYWORD,
            search_type=constants.SPOTIFY_SEARCH_TYPE_TRACK,
            limit=self.limit_items_in_call,
            offset=self.offset,
            token_type=self.token_type,
            access_token=self.access_token,
            endpoint=constants.SPOTIFY_SEARCH_ENDPOINT)

        if error is not None:
            if error == 400:
                raise SpotifyOutOfRangeWarning(
                    spotify_response_code=error,
                    spotify_response_message=search_data_batch
                )
            elif error == 401:
                raise SpotifyInvalidAccessToken(
                    spotify_response_code=error,
                    spotify_response_message=search_data_batch
                )
            else:
                raise

        else:
            if search_data_batch['tracks'] is not None \
                    and search_data_batch['tracks']['items'] is not None:
                for data in search_data_batch['tracks']['items']:
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(
                            stream=stream_name,
                            data=data,
                            emitted_at=int(datetime.now().timestamp()) * 1000))
                    state[stream_name].update({'offset': self.offset})
                    self.offset += 1
                    self.size_record_countdown -= 1
            else:
                print(f"search_data_batch: {search_data_batch}")

    def incremental_load(
            self, logger: AirbyteLogger, configured_stream: ConfiguredAirbyteStream, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        print("incremental_load is processing")

        stream_name = configured_stream.stream.name
        if state and stream_name in state:
            self.offset = state[stream_name].get('offset', 0)
        state[stream_name] = {'offset': self.offset}

        if not self.access_token_expire_time or self.access_token_expire_time < time.time():
            self.update_authentication_data()

        self.limit_items_in_call = constants.SPOTIFY_SEARCH_DEFAULT_MAX_RESPONSE_SIZE

        is_finish = False
        while not is_finish:
            try:
                yield from self.get_batch_data(logger=logger, stream_name=stream_name, state=state)

            except SpotifyOutOfRangeWarning as e:
                print(f"SpotifyOutOfRangeWarning "
                      f"limit_items_in_call = {self.limit_items_in_call}, "
                      f"{e.error_code} = {str(e.message)}")

                if self.limit_items_in_call > 1:
                    self.limit_items_in_call = int(self.limit_items_in_call / 2)
                else:
                    is_finish = True

            except SpotifyInvalidAccessToken as e:
                print(f"SpotifyInvalidAccessToken {e.error_code} = {str(e.message)}")
                self.update_authentication_data()

            except Exception as error:
                yield AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message=f"incremental_load An exception occurred: {str(error)} trace = {traceback.format_exc()}")

    def full_refresh_load(
            self, logger: AirbyteLogger, configured_stream: ConfiguredAirbyteStream,
    ) -> Generator[AirbyteMessage, None, None]:
        pass

    def read(
            self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.yaml file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """
        print("read is processing")

        self.load_config_data(config=config)

        # Not Implemented
        for configured_stream in catalog.streams:
            if configured_stream.sync_mode == SyncMode.incremental:
                yield from self.incremental_load(
                    logger=logger,
                    configured_stream=configured_stream,
                    state=state)

            else:
                yield from self.full_refresh_load(
                    logger=logger,
                    configured_stream=configured_stream)
