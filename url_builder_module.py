import configparser
from abc import ABC, abstractmethod
from loguru import logger
from typing import Dict, Optional, Union, Tuple, List, Any
from urllib.parse import urlencode

class BaseUrlBuilder(ABC):
    """
    Abstract base class for constructing and validating URLs for various APIs
    """
    BASE_URL: str # to be defined in subclass

    def __init__(self, config_file: Optional[str]=None, config_section: Optional[str]=None) -> None:
        """
        Optionally load an API from a config file
        """
        if config_file and config_section:
            config = configparser.ConfigParser()
            config.read(config_file)
            try:
                self.API_KEY = config[config_section]["API_KEY"]
            except KeyError:
                raise(ValueError(f"API key not found in '{config_file}' under section '{config_section}'."))
        else:
            self.API_KEY = None

    @abstractmethod
    def build_query_params(self, **kwargs) -> Dict[str, Any]:
        """
        Build query parameters specific to the API
        This method must be overriden by subclass
        """
    pass

    def __call__(self, **kwargs) -> str:
        """
        Constructs the complete URL using the built query parameters.
        """
        params = self.build_query_params(**kwargs)
        url = f"{self.BASE_URL}?{urlencode(params)}"
        return url

class AlphaVantageURLBuilder(BaseUrlBuilder):
    """
    URL builder for the Alpha Vantage API.
    """
    BASE_URL = 'https://www.alphavantage.co/query'
    VALID_FUNCTIONS = {'TIME_SERIES_INTRADAY', 'TIME_SERIES_DAILY', 'TIME_SERIES_WEEKLY', 'TIME_SERIES_MONTHLY'}

    def build_query_params(self, symbol: str, function: str = 'TIME_SERIES_DAILY', **kwargs) -> Dict[str, Any]:
        """
        Constructs query parameters for Alpha Vantage.
        """
        self._validate_inputs(symbol, function)
        params = {
            'apikey': self.API_KEY,
            'datatype': 'json',
            'function': function,
            'symbol': symbol
        }
        params.update(kwargs)
        return params
    
    def _validate_inputs(self, symbol: Optional[str], function: Optional[str]) -> None:
        """
        Validates that the symbol is provided and the function is among the allowed ones.
        """
        if not symbol:
            raise ValueError("Symbol must be provided")
        if function not in self.VALID_FUNCTIONS:
            raise ValueError(f"Invalid function '{function}'. Must be one of {self.VALID_FUNCTIONS}.")

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
    builder = AlphaVantageURLBuilder(config_file="keys.ini", config_section="alphavantage")
    url = builder(symbol="APPL",function="TIME_SERIES_DAILY")
    logger.info(url)

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





