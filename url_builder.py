import configparser
from loguru import logger
from typing import Optional, Union, Tuple, List
from urllib.parse import urlencode


class URLBuilder:
    """
    A functor class for constructing and validating URLs for the Alpha Vantage API.
    """
    BASE_URL = 'https://www.alphavantage.co/query'
    VALID_FUNCTIONS = {'TIME_SERIES_INTRADAY', 'TIME_SERIES_DAILY', 'TIME_SERIES_WEEKLY', 'TIME_SERIES_MONTHLY'}

    def __init__(self):
        config = configparser.ConfigParser()
        config.read("keys.ini")

        try:
            self.api_key = config["alphavantage"]["api_key"]
        except KeyError:
            raise ValueError("API key not found in keys.ini. Please ensure it is defined in the [alphavantage] section.")
    
    def __call__(self, symbol: str, function: str = 'TIME_SERIES_DAILY') -> str:
        """
        Constructs and validates the URL for the given symbol and function.
        
        Args:
            symbol (str): Ticker symbol to query.
            function (str): The Alpha Vantage API function. Defaults to daily adjusted time series.
        
        Returns:
            str: The complete URL with query parameters.
        
        Raises:
            ValueError: If symbol is missing or function is invalid.
        """
        # Automatically validate inputs
        self._validate_inputs(symbol, function)
        
        # Prepare query parameters
        query_params = {
            'apikey': self.api_key,
            'datatype': 'json',
            'function': function,
            'symbol': symbol
        }
        
        url = f"{self.BASE_URL}?{urlencode(query_params)}"
        logger.debug(f"Constructed URL: {url}")
        return url

    def _validate_inputs(self, symbol: Optional[str], function: Optional[str]) -> None:
        """
        Validates the symbol and function inputs.
        
        Args:
            symbol (Optional[str]): The ticker symbol to query.
            function (Optional[str]): The Alpha Vantage API function.
        
        Raises:
            ValueError: If symbol is missing or function is invalid.
        """
        if not symbol:
            raise ValueError("Symbol must be provided.")
        if function not in self.VALID_FUNCTIONS:
            raise ValueError(f"Invalid function '{function}'. Must be one of {self.VALID_FUNCTIONS}.")



# Test code within the module
if __name__ == "__main__":
    # Instantiate the URLBuilder
    url_builder = URLBuilder()

    # Define test cases, including both valid and invalid cases
    test_cases: List[Union[Tuple[str], Tuple[str, str]]] = [
        ("AAPL",),                    # Valid: symbol, default function
        ("GOOGL",),                   # Valid: symbol, default function
        ("IUSV", "TIME_SERIES_WEEKLY"),  # Valid: symbol with custom function
        # (None, "TIME_SERIES_WEEKLY"),    # Invalid: missing symbol
        # ("AAPL", "INVALID_FUNCTION")     # Invalid: unsupported function
    ]

    # Apply URLBuilder to each case with map and lambda
    results = map(lambda case: (case, url_builder(*case if len(case) > 1 else (case[0], "TIME_SERIES_DAILY"))), test_cases)

    # Log each result, handling exceptions gracefully
    list(map(lambda result: logger.info(f"Generated URL for {result[0]}: {result[1]}"), results))





