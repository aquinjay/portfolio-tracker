import requests
from loguru import logger
from datetime import datetime
from typing import Optional, Tuple, Union
from url_builder import URLBuilder
from joblib import Memory
import pandas as pd
import os

class DataFetcher:
    def __init__(self, default_function: str = 'TIME_SERIES_DAILY', cache_dir: str = '.cache', archive_dir: str = '.archive'):
        self.url_builder = URLBuilder()
        self.default_function = default_function
        self.memory = Memory(cache_dir, verbose=0)  # Use joblib Memory for caching
        self.archive_dir = archive_dir
        os.makedirs(archive_dir, exist_ok=True)  # Ensure the archive directory exists
        self.session = requests.Session()  # Persistent session for HTTP requests

    def __call__(self, symbol: str, function: Optional[str] = None, date_range: Optional[Tuple[str, str]] = None, full_refresh: bool = False) -> Union[dict, pd.DataFrame]:
        function = function or self.default_function
        cache_key = f"{symbol}_{function}"

        # Handle full refresh: Clear the cache for this symbol/function
        if full_refresh:
            logger.info(f"Performing full refresh for {symbol} with function '{function}'.")
            self.memory.clear(warn=False)  # Clear all cached items in the cache directory

        # Load cached data if available
        cached_data = self._load_cached_data(cache_key)
        
        # Check if cached data covers the latest trading day
        if cached_data is not None:
            max_cached_date = pd.to_datetime(cached_data['date'].max())
            latest_trading_day = self.get_latest_trading_day()
            
            if max_cached_date >= latest_trading_day:
                logger.info(f"Using cached data for {symbol} with function '{function}' up to {max_cached_date}.")
                return self.filter_by_date(cached_data, date_range)
            else:
                # Archive old cached data before updating
                self._archive_old_data(cached_data, cache_key)

        # Fetch new data if needed
        try:
            url = self.url_builder(symbol, function)
            logger.info(f"Fetching new data for {symbol} with function '{function}' from URL: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            new_data = self.process_data(response.json())
            
            # Update cache with new data and return filtered results
            self._update_cache(cache_key, new_data)
            return self.filter_by_date(new_data, date_range)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for {symbol}: {e}")
            return {}

    def get_latest_trading_day(self) -> datetime:
        """Returns the latest trading day based on today's date."""
        today = datetime.today()
        return today if today.weekday() < 5 else today - pd.DateOffset(days=today.weekday() - 4)

    def process_data(self, data: dict) -> pd.DataFrame:
        """Processes the raw JSON data into a DataFrame format."""
        daily_data = data.get('Time Series (Daily)', {})
        records = [{'date': date, 'close': float(values['4. close'])} for date, values in daily_data.items()]
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values(by='date', inplace=True)
        return df

    def filter_by_date(self, df: pd.DataFrame, date_range: Optional[Tuple[str, str]]) -> pd.DataFrame:
        """Filters the DataFrame by the specified date range."""
        if date_range:
            start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
            mask = (df['date'] >= start_date) & (df['date'] <= end_date)
            return df[mask]
        return df

    def _load_cached_data(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Loads cached data if available."""
        try:
            cached_data = self.memory.cache(pd.DataFrame)(lambda: pd.DataFrame())(cache_key)  # Returns cached if exists
            return cached_data if not cached_data.empty else None
        except FileNotFoundError:
            return None

    def _update_cache(self, cache_key: str, data: pd.DataFrame):
        """Updates the cache with new data."""
        self.memory.cache(pd.DataFrame)(lambda x: x)(data, cache_key)
        logger.info(f"Updated cache for {cache_key}")

    def _archive_old_data(self, old_data: pd.DataFrame, cache_key: str):
        """Archives the old cached data to a separate directory."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = os.path.join(self.archive_dir, f"{cache_key}_{timestamp}.csv")
        old_data.to_csv(archive_path, index=False)
        logger.info(f"Archived old data for {cache_key} to {archive_path}")
