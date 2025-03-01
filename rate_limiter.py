import time
from functools import wraps
from loguru import logger

def rate_limited(delay: float = 0):
    """
    A decorator that adds a delay after the function call.
    
    Args:
        delay (float): Number of seconds to wait after the function call. Default is 0.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            logger.info(f"Sleeping for {delay} seconds after calling {func.__name__}...")
            time.sleep(delay)
            return result
        return wrapper
    return decorator

# Test code for the rate limiter decorator.
if __name__ == "__main__":
    # Set up the logger to output to the console.
    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level="INFO")
    
    @rate_limited(delay=0)  # Using the default delay of 0 seconds.
    def test_func(x):
        logger.info(f"test_func called with x = {x}")
        return x * 2

    start_time = time.time()
    
    # Call the function several times in a loop.
    for i in range(5):
        result = test_func(i)
        logger.info(f"Result: {result}")

    end_time = time.time()
    elapsed = end_time - start_time
    logger.info(f"Total elapsed time: {elapsed:.2f} seconds")
