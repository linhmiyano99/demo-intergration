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

from .spotify.utils import SpotifyInvalidAccessToken, SpotifyOutOfRangeWarning
from .spotify import utils, constants


class SourceSpotify(Source):
    def __init__(self):
        self.access_token = None
        self.token_type = None
        self.access_token_expire_time = None
        self.client_id = None
        self.client_secret = None
        self.offset = 0
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
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": config,
        }

        # Not Implemented

        streams.append(
            AirbyteStream(
                name=stream_name,
                json_schema=json_schema,
                supported_sync_modes=[SyncMode.incremental],
                default_cursor_field=["overwrite"]))
        return AirbyteCatalog(streams=streams)

    def get_batch_data(
            self, stream_name: str, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:

        search_data_batch, error = utils.get_spotify_search_data(
            query=constants.SPOTIFY_SEARCH_KEYWORD,
            search_type=constants.SPOTIFY_SEARCH_TYPE_TRACK,
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
                self.update_authentication_data()
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
                            data={'$data': data},
                            emitted_at=int(datetime.now().timestamp()) * 1000))
                    state[stream_name].update({'offset': self.offset})
                    self.offset += 1
                    self.size_record_countdown -= 1
            else:
                print(search_data_batch)

    def incremental_load(
            self, logger: AirbyteLogger, configured_stream: ConfiguredAirbyteStream, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        stream_name = configured_stream.stream.name
        if state and stream_name in state:
            self.offset = state[stream_name].get('offset', 0)
        state[stream_name] = {'offset': self.offset}
        is_finish = False

        if not self.access_token_expire_time or self.access_token_expire_time < time.time():
            self.update_authentication_data()

        while not is_finish:

            is_success = False
            retrial = 0
            while retrial < 2 and not is_success:
                retrial += 1

                try:
                    yield from self.get_batch_data(stream_name=stream_name, state=state)
                    is_success = True

                except SpotifyOutOfRangeWarning:
                    is_success = True
                    is_finish = True

                except SpotifyInvalidAccessToken:
                    pass
                except Exception as error:
                    yield AirbyteConnectionStatus(
                        status=Status.FAILED,
                        message=f"An exception occurred: {error} trace = {traceback.format_exc()}")
                    time.sleep(2)

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
