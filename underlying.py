from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional

class BaseUnderlying(ABC):
    """
    Abstract base class for underlying assets.
    """
    def __init__(self, symbol: str, name: str = "", instrument_type: str = "equity") -> None:
        """
        Initialize the underlying with identification and basic info.
        """
        self.symbol = symbol
        self.name = name
        self.instrument_type = instrument_type
        self.last_updated: Optional[datetime] = None
        self.data: Dict[str, Any] = {}  # Container for raw market data

    @abstractmethod
    def update_market_data(self) -> None:
        """
        Fetch and update the latest market data for the underlying.
        This might include the current price, volume, etc.
        """
        pass

    @abstractmethod
    def get_current_price(self) -> float:
        """
        Retrieve the current market price of the underlying.
        """
        pass

    def __repr__(self) -> str:
        return f"{self.instrument_type.capitalize()}({self.symbol})"
