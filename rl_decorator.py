import time
from functools import wraps
from loguru import logger

def rate_limited(max_requests_per_minute: int):
    """
    A decorator that limits the number of calls to a function to at most
    max_requests_per_minute using a sliding-window algorithm.
    
    Args:
        max_requests_per_minute (int): Maximum number of calls allowed in one minute.
        
    Returns:
        The decorated function.
    """
    interval = 60.0  # seconds per minute
    call_timestamps = []  # list to store timestamps of recent calls

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal call_timestamps
            now = time.time()
            # Remove timestamps older than one minute.
            call_timestamps = [t for t in call_timestamps if now - t < interval]
            if len(call_timestamps) >= max_requests_per_minute:
                # Calculate the time to wait until the oldest call is older than one minute.
                sleep_time = interval - (now - call_timestamps[0])
                logger.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                now = time.time()
                # Clean the timestamps again after sleeping.
                call_timestamps = [t for t in call_timestamps if now - t < interval]
            call_timestamps.append(now)
            return func(*args, **kwargs)
        return wrapper
    return decorator

# ------------------------------------------------------------------------------
# Test code for the rate_limited decorator
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    # Set up the logger to print INFO messages to the console.
    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level="INFO")
    
    @rate_limited(5)  # For testing, limit to 5 calls per minute.
    def test_api_call(x):
        logger.info(f"API call with x = {x}\n")
        return x * 2

    start_time = time.time()
    results = []
    
    # Attempt to call test_api_call 10 times.
    for i in range(10):
        results.append(test_api_call(i))
    
    end_time = time.time()
    logger.info(f"Results: {results}\n")
    logger.info(f"Total elapsed time: {end_time - start_time:.2f} seconds\n")
