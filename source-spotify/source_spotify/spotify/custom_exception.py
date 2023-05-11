class SpotifyOutOfRangeWarning(Exception):
    def __init__(self, spotify_response_code, spotify_response_message):
        self.error_code = spotify_response_code
        self.message = spotify_response_message


class SpotifyInvalidAccessToken(Exception):
    def __init__(self, spotify_response_code, spotify_response_message):
        self.error_code = spotify_response_code
        self.message = spotify_response_message
