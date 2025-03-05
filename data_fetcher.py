import requests
import pandas as pd
from loguru import logger
from typing import Optional
from url_builder_module import AlphaVantageURLBuilder  # Import URLBuilder for URL construction
from cache_decorator import cache_decorator
from rl_decorator import rate_limited

# Optionally define a custom key function if needed.
def my_cache_key(*args, **kwargs) -> str:
    symbol = args[0] if len(args) > 0 else kwargs.get("symbol")
    function = kwargs.get("function", "TIME_SERIES_DAILY")
    if function == "HISTORICAL_OPTIONS" and "date" in kwargs:
        date_val = kwargs["date"]
        return f"{symbol}_{function}_{date_val}"
    return f"{symbol}_{function}"

@cache_decorator(key_func=my_cache_key, use_cache=True)
@rate_limited(5)
def fetch_data(
    symbol: str, 
    function: str = "TIME_SERIES_DAILY", 
    builder = None,
    **kwargs
) -> Optional[pd.DataFrame]:
    """
    Fetches data from the API for a given symbol and returns it as a DataFrame.
    Caching is handled by the decorator.
    
    For equity-related functions (default), it expects the JSON to include
    "Time Series (Daily)". For historical options (function == "HISTORICAL_OPTIONS"),
    it expects a JSON structure with a "data" key containing a list of contract records.
    
    Args:
        symbol (str): The stock symbol to fetch data for.
        function (str): The Alpha Vantage function type. Defaults to "TIME_SERIES_DAILY".
        builder: An optional URL builder instance.
        **kwargs: Additional keyword arguments (e.g., 'date' for historical options).
    
    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the data if successful, None otherwise.
    """
    if builder is None:
        from url_builder_module import AlphaVantageURLBuilder
        builder = AlphaVantageURLBuilder(config_file="keys.ini", config_section="alphavantage")
    
    try:
        url = builder(symbol, function, **kwargs)
    except ValueError as e:
        logger.error(f"Error constructing URL: {e}")
        return None

    try:
        logger.info(f"Fetching data for {symbol} from API.")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Branch based on the function type.
        if function == "HISTORICAL_OPTIONS":
            # Expect the JSON to have a "data" key with a list of options records.
            if "data" not in data:
                logger.error(f"No valid data found for {symbol} (Historical Options). Response keys: {list(data.keys())}")
                return None
            df = pd.DataFrame(data["data"])
        else:
            # Default: expect "Time Series (Daily)".
            if "Time Series (Daily)" not in data:
                logger.error(f"No valid data found for {symbol}. Response keys: {list(data.keys())}")
                return None
            df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")
            df = df.sort_index()

        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data for {symbol} due to a request error: {e}")
        return None
    except ValueError as e:
        logger.error(f"Failed to parse data for {symbol}: {e}")
        return None

if __name__ == "__main__":
    # Test fetching data for equity (default)
    test_symbol = "AAPL"
    data = fetch_data(test_symbol)
    if data is not None:
        logger.info(f"Fetched data for {test_symbol}:\n{data.head()}")
    else:
        logger.warning(f"Failed to fetch data for {test_symbol}.")

    # Test fetching historical options data.
    try:
        logger.info("Testing historical options data pull")
        data_options = fetch_data(test_symbol, function="HISTORICAL_OPTIONS", date="2025-02-20")
        logger.info(f"Fetched historical options data for {test_symbol}:\n{data_options.head()}")
    except Exception as e:
        logger.error(f"Error fetching historical options data for {test_symbol}: {e}")
