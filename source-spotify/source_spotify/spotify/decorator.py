import time

from source_spotify.spotify.custom_exception import SpotifyOutOfRangeWarning, SpotifyInvalidAccessToken


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
