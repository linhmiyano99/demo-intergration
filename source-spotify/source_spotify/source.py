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

from source_spotify.spotify.decorator import SpotifyInvalidAccessToken, SpotifyOutOfRangeWarning, backoff
from source_spotify.spotify import constants
from source_spotify.spotify.spotify_api import SpotifyAPI


class SourceSpotify(Source):

    def __init__(self):
        self.offset = 0
        self.limit_items_in_call = None
        self.spotify_api = SpotifyAPI()

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
        self.spotify_api.load_config_data(config=config)
        try:
            # Not Implemented
            error = self.spotify_api.update_authentication_data()
            if error is not None:
                error = f"error: {error}"
                print(error)
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message=f"An exception occurred: {error} trace = {traceback.format_exc()}")

            _, error = self.spotify_api.get_spotify_search_data(
                query=constants.SPOTIFY_SEARCH_KEYWORD,
                search_type=constants.SPOTIFY_SEARCH_TYPE_TRACK,
                limit=constants.SPOTIFY_SEARCH_DEFAULT_LIMIT,
                offset=constants.SPOTIFY_SEARCH_START_DEFAULT_OFFSET)

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

        stream_name = "search.tracks"
        json_schema = {  # Example
            "$schema": "http://json-schema.org/draft-03/schema#",
            "type": "object",
            "properties": self.spotify_api.get_search_track_properties_default()
        }

        # Not Implemented

        streams.append(
            AirbyteStream(
                name=stream_name,
                json_schema=json_schema,
                supported_sync_modes=[SyncMode.incremental]))
        return AirbyteCatalog(streams=streams)

    @backoff(retries=2, delay=1)
    def get_batch_data(
            self, logger: AirbyteLogger, stream_name: str, state: Dict[str, any], timestamp: str
    ) -> Generator[AirbyteMessage, None, None]:
        print("get_batch_data is processing")

        search_data_batch, error = self.spotify_api.get_spotify_search_data(
            query=constants.SPOTIFY_SEARCH_KEYWORD,
            search_type=constants.SPOTIFY_SEARCH_TYPE_TRACK,
            limit=self.limit_items_in_call,
            offset=self.offset)

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
                    event = SpotifyAPI.get_event_from_track(
                        track=data,
                        namespace=constants.NAMESPACE,
                        timestamp=timestamp
                    )

                    print(f"event info to destination {event}")

                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(
                            stream=stream_name,
                            data=event,
                            emitted_at=int(datetime.now().timestamp()) * 1000))
                    state[stream_name].update({'offset': self.offset})
                    self.offset += 1
            else:
                print(f"search_data_batch: {search_data_batch}")

    def incremental_load(
            self, logger: AirbyteLogger, configured_stream: ConfiguredAirbyteStream, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        print("incremental_load is processing")

        timestamp = SpotifyAPI.get_current_timestamp()

        stream_name = configured_stream.stream.name
        if state and stream_name in state:
            self.offset = state[stream_name].get('offset', 0)
        state[stream_name] = {'offset': self.offset}

        if not self.spotify_api.access_token_expire_time or self.spotify_api.access_token_expire_time < time.time():
            self.spotify_api.update_authentication_data()

        self.limit_items_in_call = constants.SPOTIFY_SEARCH_DEFAULT_MAX_RESPONSE_SIZE

        is_finish = False
        while not is_finish:
            try:
                yield from self.get_batch_data(
                    logger=logger,
                    stream_name=stream_name,
                    state=state,
                    timestamp=timestamp
                )

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
                self.spotify_api.update_authentication_data()

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

        self.spotify_api.load_config_data(config=config)

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
