import pandas as pd
from pandas.tseries.holiday import (
    USFederalHolidayCalendar,
    AbstractHolidayCalendar,
    Holiday,
    nearest_workday,
)
from pandas.tseries.offsets import DateOffset, Easter
from loguru import logger

def fourth_friday_november(dt):
    """Helper function to calculate the fourth Friday of November."""
    return (pd.Timestamp(year=dt.year, month=11, day=1) + DateOffset(weekday=4, weeks=3))

class USMarketHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday),
        Holiday("Martin Luther King Jr. Day", month=1, day=1, offset=DateOffset(weekday=2)),
        Holiday("Washington's Birthday", month=2, day=1, offset=DateOffset(weekday=2)),
        Holiday("Good Friday", month=1, day=1, offset=Easter(-2)),  # Offset Good Friday as 2 days before Easter
        Holiday("Memorial Day", month=5, day=31, offset=DateOffset(weekday=0)),
        Holiday("Independence Day", month=7, day=4, observance=nearest_workday),
        Holiday("Labor Day", month=9, day=1, offset=DateOffset(weekday=0)),
        Holiday("Thanksgiving", month=11, day=1, offset=DateOffset(weekday=3)),
        Holiday("Day after Thanksgiving", month=11, day=1, observance=fourth_friday_november),
        Holiday("Christmas", month=12, day=25, observance=nearest_workday),
    ]


class JapanMarketHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        # New Year's Holiday in Japan (usually observed on January 1-3)
        Holiday("New Year's Holiday", month=1, day=1, offset=DateOffset(days=0)),
        # Respect for the Aged Day (third Monday in September)
        Holiday("Respect for the Aged Day", month=9, day=1, offset=DateOffset(weekday=0)),
        # And other Japanese public holidays observed by the Tokyo Stock Exchange...
    ]

# Define UK Market Holidays (example)
class UKMarketHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday),
        Holiday("Good Friday", month=1, day=1, offset=pd.offsets.Easter(-2)),
        Holiday("Early May Bank Holiday", month=5, day=1, offset=DateOffset(weekday=0)),
        Holiday("Spring Bank Holiday", month=5, day=31, offset=DateOffset(weekday=0)),
        Holiday("Christmas", month=12, day=25, observance=nearest_workday),
        Holiday("Boxing Day", month=12, day=26, observance=nearest_workday),
    ]

market_holiday_calendars = {
    "US": USMarketHolidayCalendar(),
    "Japan": JapanMarketHolidayCalendar(),
    "UK": UKMarketHolidayCalendar(),
    # Add other markets as needed
}


if __name__ == "__main__":
    from datetime import datetime

    # Test USMarketHolidayCalendar
    us_calendar = USMarketHolidayCalendar()
    year = datetime.now().year

    # Generate and print holiday dates for the current year
    us_holidays = us_calendar.holidays(start=f"{year}-01-01", end=f"{year}-12-31")

    logger.info("US Market Holidays for the year:", year)
    for holiday in us_holidays:
        logger.info(pd.Timestamp(holiday))

    # Sample expected output (for visual verification):
    # You might see dates like:
    # - New Year's Day: 2023-01-02 (observed)
    # - Martin Luther King Jr. Day: 2023-01-16
    # - Washington's Birthday: 2023-02-20
    # - Good Friday: 2023-04-07
    # - Memorial Day: 2023-05-29
    # - Independence Day: 2023-07-04
    # - Labor Day: 2023-09-04
    # - Thanksgiving: 2023-11-23
    # - Day after Thanksgiving: 2023-11-24
    # - Christmas: 2023-12-25
