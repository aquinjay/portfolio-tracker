import configparser
from abc import ABC, abstractmethod
from loguru import logger
from typing import Dict, Optional, Union, Tuple, List, Any
from urllib.parse import urlencode

class BaseUrlBuilder(ABC):
    """
    Abstract base class for constructing and validating URLs for various APIs.
    """
    BASE_URL: str  # To be defined in subclass

    def __init__(self, config_file: Optional[str] = None, config_section: Optional[str] = None) -> None:
        """
        Optionally load an API key from a config file.
        """
        if config_file and config_section:
            config = configparser.ConfigParser()
            config.read(config_file)
            try:
                self.API_KEY = config[config_section]["API_KEY"]
            except KeyError:
                raise ValueError(f"API key not found in '{config_file}' under section '{config_section}'.")
        else:
            self.API_KEY = None

    @abstractmethod
    def build_query_params(self, **kwargs) -> Dict[str, Any]:
        """
        Build query parameters specific to the API.
        This method must be overridden by a subclass.
        """
        pass

    def __call__(self, *args,**kwargs) -> str:
        """
        Constructs the complete URL using the built query parameters.
        """
        params = self.build_query_params(*args, **kwargs)
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
        Validates that the symbol is provided and the function is allowed.
        """
        if not symbol:
            raise ValueError("Symbol must be provided.")
        if function not in self.VALID_FUNCTIONS:
            raise ValueError(f"Invalid function '{function}'. Must be one of {self.VALID_FUNCTIONS}.")

# Test code within the module
if __name__ == "__main__":
    # Instantiate the AlphaVantageURLBuilder
    builder = AlphaVantageURLBuilder(config_file="keys.ini", config_section="alphavantage")
    
    # Generate and log a URL for a single case
    url = builder(symbol="APPL", function="TIME_SERIES_DAILY")
    logger.info(url)

    # Define test cases, including both valid and invalid cases
    test_cases: List[Union[Tuple[str], Tuple[str, str]]] = [
        ("AAPL",),                     # Valid: symbol, default function
        ("GOOGL",),                    # Valid: symbol, default function
        ("IUSV", "TIME_SERIES_WEEKLY"), # Valid: symbol with custom function
        # (None, "TIME_SERIES_WEEKLY"),    # Invalid: missing symbol
        # ("AAPL", "INVALID_FUNCTION")     # Invalid: unsupported function
    ]

    def safe_build(case: Union[Tuple[str], Tuple[str, str]]):
        """
        Attempts to generate a URL for the given test case.
        If an error occurs, returns an error message.
        """
        try:
            if len(case) > 1:
                return case, builder(*case)
            else:
                # Explicitly supply the default function when only symbol is provided
                return case, builder(case[0], "TIME_SERIES_DAILY")
        except Exception as e:
            return case, f"Error: {e}"

    # Apply the URL builder to each test case and log the results
    results = map(safe_build, test_cases)
    for case, result in results:
        logger.info(f"Generated URL for {case}: {result}")
