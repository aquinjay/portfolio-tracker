import requests
import pandas as pd
from loguru import logger
from typing import Optional
from url_builder_module import AlphaVantageURLBuilder # Import URLBuilder for URL construction
from cache_manager import CacheManager  # Import CacheManager for caching functionality

# Initialize URLBuilder and CacheManager
cache_manager = CacheManager(cache_dir=".cache", archive_dir=".archive")

def fetch_data(
    symbol: str, 
    function: str = "TIME_SERIES_DAILY", 
    cache: bool = True, 
    builder = None
    ) -> Optional[pd.DataFrame]:
    """
    Fetches data from the API for a given symbol and returns it as a DataFrame.
    Optionally uses caching to avoid redundant API calls.

    Args:
        symbol (str): The stock symbol to fetch data for.
        function (str): The Alpha Vantage function type, defaults to "TIME_SERIES_DAILY".
        cache (bool): Whether to use caching. Defaults to True.

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the stock data if successful, None otherwise.
    """
    if builder is None:
        from url_builder_module import AlphaVantageURLBuilder
        builder = AlphaVantageURLBuilder(config_file="keys.ini", config_section="alphavantage")

    cache_key = f"{symbol}_{function}"

    # Check cache first if caching is enabled
    if cache and cache_manager.is_data_up_to_date(cache_key):
        logger.info(f"Loading cached data for {symbol}.")
        cached_data = cache_manager.load_cached_data(cache_key)
        if cached_data is not None:
            return cached_data

    # Use URLBuilder to construct the URL
    try:
        url = builder(symbol, function)
    except ValueError as e:
        logger.error(f"Error constructing URL: {e}")
        return None

    try:
        logger.info(f"Fetching data for {symbol} from API.")
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for unsuccessful status codes

        data = response.json()

        # Validate and process data
        if "Time Series (Daily)" not in data:
            logger.error(f"No valid data found for {symbol}. Response: {data}")
            return None

        df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")
        # df.index = pd.to_datetime(df.index)  # Ensure the index is in datetime format
        df = df.sort_index()  # Sort data by date

        # Cache the data if caching is enabled
        if cache:
            logger.info(f"Caching data for {symbol}.")
            cache_manager.save_to_cache(cache_key, df)

        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data for {symbol} due to a request error: {e}")
        return None
    except ValueError as e:
        logger.error(f"Failed to parse data for {symbol}: {e}")
        return None


if __name__ == "__main__":
    # Test fetching data for a specific symbol
    test_symbol = "AAPL"
    data = fetch_data(test_symbol)

    if data is not None:
        logger.info(f"Fetched data for {test_symbol}:\n{data.head()}")
    else:
        logger.warning(f"Failed to fetch data for {test_symbol}.")

