def backoff(retries=2):
    def decorator(func):
        def wrapper(*args, **kwargs):
            is_success = False
            retrial = 0
            while retrial < retries and not is_success:
                retrial += 1
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    raise error
        return wrapper
    return decorator
