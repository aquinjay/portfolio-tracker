from diskcache import FanoutCache, Disk
import os
import psutil
import pandas as pd
from market_holidays import market_holiday_calendars
from datetime import datetime, timedelta
from loguru import logger
from typing import Optional

class CacheManager:
    def __init__(self, cache_dir: str = '.cache', archive_dir: str = '.archive'):
        # Set size limit to one-quarter of total system RAM
        total_ram = psutil.virtual_memory().total
        size_limit = total_ram // 4

        # Initialize FanoutCache with disk persistence and memory limit
        self.cache = FanoutCache(cache_dir, disk=Disk, size_limit=size_limit)
        self.archive_dir = archive_dir
        os.makedirs(archive_dir, exist_ok=True)
        logger.info(f"Initialized cache with size limit set to {size_limit} bytes (one-quarter of total RAM)")

 
    def is_data_up_to_date(self, cache_key: str) -> bool:
        try:
            cached_data = self.load_cached_data(cache_key)
    
            # Check if cached_data exists and has a 'date' column
            if cached_data is None:
                logger.warning(f"No cached data found for {cache_key}.")
                return False

            if 'date' in cached_data.columns:
                cached_data['date'] = pd.to_datetime(cached_data['date'], errors='coerce')
                max_cached_date = cached_data['date'].max()
            else:
                cached_data.index = pd.to_datetime(cached_data.index, errors='coerce')
                max_cached_date = cached_data.index.max()

            if isinstance(max_cached_date, pd.Timestamp) and pd.isna(max_cached_date):
                logger.info(f"Cached data for {cache_key} contains no valid dates.")
                return False

            # Convert max_cached_date to date if it’s a Timestamp for comparison
            if isinstance(max_cached_date, pd.Timestamp):
                if pd.isna(max_cached_date):
                    logger.info(f"Cached data for {cache_key} contains no valid dates.")
                    return False
                max_cached_date = max_cached_date.date()
            else:
                logger.info(f"max_cached_date is not a Timestamp. Setting it to None.")
                max_cached_date = None

            if max_cached_date is None:
                return False
            
            latest_trading_day = self.get_latest_trading_day().date()
    
            # Check if max_cached_date is up-to-date
            if max_cached_date < latest_trading_day:
                logger.info(f"Cached data for {cache_key} is outdated.")
                return False
    
            return True
    
        except Exception as e:
            # Log the exception and return False to prevent crashing
            logger.error(f"Error checking if data is up-to-date for {cache_key}: {e}")
            return False

    def load_cached_data(self, cache_key: str) -> Optional[pd.DataFrame]:
        """
        Loads cached data if available and moves it to memory if accessed.
        """
        cached_data = self.cache.get(cache_key)

        # Move data to memory if accessed
        if isinstance(cached_data, pd.DataFrame) and not cached_data.empty:
            logger.info(f"Moved {cache_key} from disk to memory for faster access")
            return cached_data
        else:
            return None

    def save_to_cache(self, cache_key: str, data: pd.DataFrame):
        """
        Caches the provided data and ensures it's initially stored in memory.
        """
        # Store data in cache; this will start in memory if within size limit
        self.cache.set(cache_key, data)
        logger.info(f"Data saved to cache for {cache_key}")

    def archive_data(self, cache_key: str, old_data: pd.DataFrame):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = os.path.join(self.archive_dir, f"{cache_key}_{timestamp}.csv")
        old_data.to_csv(archive_path, index=False)
        logger.info(f"Archived old data for {cache_key} to {archive_path}")

    def clear_cache_for_key(self, cache_key: str):
        if cache_key in self.cache:
            del self.cache[cache_key]
            logger.info(f"Cleared cache for {cache_key}")
        else:
            logger.info(f"No cache entry found for {cache_key} to clear.")

    def clear_all_cache(self):
        self.cache.clear()
        logger.info("Cleared the entire cache")

    def get_latest_trading_day(self, market: str = "US") -> datetime:
        # Select the appropriate holiday calendar based on the market
        calendar = market_holiday_calendars.get(market, market_holiday_calendars["US"])  # Default to US calendar if not found
        today = datetime.today()
        potential_trading_day = today if today.weekday() < 5 else today - pd.DateOffset(days=today.weekday() - 4)
    
        # Generate a holiday schedule for the year
        holidays = calendar.holidays(start=potential_trading_day - pd.DateOffset(years=1), end=potential_trading_day)
    
        # Roll back to the most recent trading day if today or any day lands on a holiday or weekend
        while potential_trading_day in holidays or potential_trading_day.weekday() >= 5:
            potential_trading_day -= pd.DateOffset(days=1)
    
        return potential_trading_day

            
    def close(self):
        self.cache.close()
        logger.info("Flushed in-memory cache to disk on exit.")


if __name__ == "__main__":
    cache_manager = CacheManager(cache_dir='.test_cache', archive_dir='.test_archive')
    test_cache_key = "AAPL_TEST"
    sample_data = pd.DataFrame({
        'date': [datetime.today() - timedelta(days=i) for i in range(5)],
        'close': [150 + i for i in range(5)]
    })

    # Save data to cache
    cache_manager.save_to_cache(test_cache_key, sample_data)

    # Display cache stats after saving
    logger.info("Cache stats after saving data:")

    # Load data from cache
    loaded_data = cache_manager.load_cached_data(test_cache_key)
    logger.info(f"Loaded data:\n{loaded_data}")

    # Display cache stats after loading data
    logger.info("Cache stats after loading data:")

    # Clear specific cache entry and display stats
    cache_manager.clear_cache_for_key(test_cache_key)
    logger.info("Cache stats after clearing specific key:")

    # Test case: Data with date as index
    test_cache_key_index = "AAPL_TEST_INDEX"
    sample_data_index = pd.DataFrame({
        'close': [150 + i for i in range(5)]
    })
    sample_data_index.index = pd.to_datetime([datetime.today() - timedelta(days=i) for i in range(5)])

    # Save data with date as index to cache
    cache_manager.save_to_cache(test_cache_key_index, sample_data_index)
    logger.info(f"Testing with date as index. Cached data:\n{sample_data_index}")

    # Check if cached data with date as index is up-to-date
    is_up_to_date = cache_manager.is_data_up_to_date(test_cache_key_index)
    logger.info(f"Is cached data (date as index) up-to-date? {is_up_to_date}")

    # Clear all cache and display final stats
    cache_manager.clear_all_cache()
    logger.info("Final cache stats after clearing all cache:")

    cache_manager.clear_cache_for_key(test_cache_key_index)
    cache_manager.clear_cache_for_key(test_cache_key)
    cache_manager.close()
