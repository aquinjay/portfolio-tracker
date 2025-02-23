# caching/cache_decorator.py
from functools import wraps
from typing import Callable, Any
from loguru import logger
from cache_manager import CacheManager  # Import your CacheManager
import atexit

# Create a global CacheManager instance
cache_manager = CacheManager(cache_dir=".cache", archive_dir=".archive")
atexit.register(cache_manager.close)

def cache_decorator(key_func: Callable[..., str] = None, use_cache: bool = True):
    """
    A decorator that caches the results of a function call.
    
    Args:
        key_func (Callable[..., str], optional): Function to generate a cache key.
        use_cache (bool): Whether to enable caching.
        
    Returns:
        The decorated function.
    """
    def decorator(func: Callable[..., Any]):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # If caching is disabled, run the function directly.
            if not use_cache:
                logger.info("Ran decorator")
                return func(*args, **kwargs)
            
            # Generate a cache key.
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default key generation: assume the first argument is 'symbol'
                # and the second (optional) is the 'function' type.
                symbol = args[0] if len(args) > 0 else kwargs.get("symbol")
                function = args[1] if len(args) > 1 else kwargs.get("function", "TIME_SERIES_DAILY")
                cache_key = f"{symbol}_{function}"
            
            # Check if cached data is up-to-date.
            if cache_manager.is_data_up_to_date(cache_key):
                logger.info(f"Cache hit for {cache_key}.")
                cached_data = cache_manager.load_cached_data(cache_key)
                if cached_data is not None:
                    return cached_data
            
            # Call the actual function and cache its result.
            result = func(*args, **kwargs)
            if result is not None:
                cache_manager.save_to_cache(cache_key, result)
            return result
        
        return wrapper
    return decorator

# ------------------ Test Code Below ------------------
if __name__ == "__main__":
    import pandas as pd
    
    from cache_manager import CacheManager  # Import your CacheManager
    cache_manager = CacheManager(cache_dir=".test_cache", archive_dir=".test_archive")
    # Global counter to track actual function calls.
    call_counter = 0
    from datetime import datetime

    @cache_decorator()
    def expensive_function(x: int) -> pd.DataFrame:
        """
        Dummy function simulating an expensive computation or API call.
        It returns a DataFrame based on the input.
        """
        global call_counter
        call_counter += 1
        now = datetime.now()
        data = pd.DataFrame({
            "date": [now for _ in range(10)],
            "value": [x * i for i in range(10)]
        })
        data.set_index("date", inplace=True)
        return data

    def test_cache_decorator():
        """
        Test the cache_decorator to ensure that repeated calls with the same input
        return cached results instead of re-executing the function.
        """
        global call_counter
        call_counter = 0
        
        # Clear all cached data before testing.
        cache_manager.clear_all_cache()
        
        # First call should execute the function and cache the result.
        result1 = expensive_function(5)
        # Second call with the same parameter should hit the cache.
        result2 = expensive_function(5)
        
        # Verify the function was executed only once.
        assert call_counter == 1, f"Expected 1 call, got {call_counter}"
        # Verify that both results are identical.
        pd.testing.assert_frame_equal(result1, result2)
        
        # Calling with a different parameter should cause a new execution.
        result3 = expensive_function(10)
        assert call_counter == 2, f"Expected 2 calls, got {call_counter}"
        
        logger.info("Cache decorator test passed.")

    # Run the tests
    test_cache_decorator()
