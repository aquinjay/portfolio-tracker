import os
import pickle
import pandas as pd
from datetime import datetime
from loguru import logger
from typing import Dict, Optional

# Assume fetch_data is defined in your data_fetcher module.
from data_fetcher import fetch_data

class HistoricalOptionsDataProcessor:
    """
    A processor for historical options data for a single underlying.
    
    When run, it will:
      1. Fetch raw data snapshots for a specified date range.
      2. Process the raw data (currently, a simple copy).
      3. Organize the data by contractID (grouping all snapshots for each contract).
      4. Automatically save the raw and processed data to pickle files using a naming convention
         that reflects the full date range of the stored data.
      5. Merge new data with existing saved data so that data is appended rather than overwritten.
    """
    def __init__(self, symbol: str, start_date: str, end_date: str, archive_dir: Optional[str] = None, freq: str = 'B'):
        """
        Args:
            symbol (str): The underlying symbol (e.g., "IBM").
            start_date (str): The initial start date ("YYYY-MM-DD").
            end_date (str): The initial end date ("YYYY-MM-DD").
            archive_dir (str, optional): Directory to store pickle files. Defaults to ".processed_archive".
            freq (str): Frequency for iterating over dates (e.g., 'B' for business days).
        """
        self.symbol = symbol
        self.initial_start_date = pd.to_datetime(start_date)
        self.initial_end_date = pd.to_datetime(end_date)
        self.archive_dir = archive_dir if archive_dir is not None else ".processed_archive"
        os.makedirs(self.archive_dir, exist_ok=True)
        self.freq = freq
        self.function = "HISTORICAL_OPTIONS"  # Dedicated API function
        
        self.raw_data: Dict[str, pd.DataFrame] = {}       # Raw snapshots keyed by snapshot date (string)
        self.processed_data: Dict[str, pd.DataFrame] = {}   # Processed data keyed by snapshot date

    def process_data(self) -> None:
        """
        Loops through the date range (using the specified frequency) and fetches data.
        Each snapshot is stored in self.raw_data, keyed by its date string.
        """
        date_range = pd.date_range(self.initial_start_date, self.initial_end_date, freq=self.freq)
        logger.info(f"Fetching historical options data for {self.symbol} from {self.initial_start_date.date()} to {self.initial_end_date.date()} ({len(date_range)} dates).")
        for single_date in date_range:
            date_str = single_date.strftime("%Y-%m-%d")
            logger.info(f"Fetching data for {date_str}...")
            try:
                df = fetch_data(self.symbol, function=self.function, date=date_str)
                if df is not None and not df.empty:
                    self.raw_data[date_str] = df
                    logger.info(f"Data for {date_str}: {len(df)} records fetched.")
                else:
                    logger.warning(f"No data returned for {date_str}.")
            except Exception as e:
                logger.error(f"Error fetching data for {date_str}: {e}")

    def process_raw_data(self) -> None:
        """
        Processes the raw data to create processed data.
        Currently, it simply copies raw_data to processed_data.
        (Additional cleaning can be added here later.)
        """
        self.processed_data = self.raw_data.copy()
        logger.info("Raw data copied to processed data.")

    def organize_data_by_contract(self) -> Dict[str, pd.DataFrame]:
        """
        Organizes the raw data snapshots by contractID.
        
        It concatenates all snapshots (each tagged with its snapshot_date), converts
        the expiration field to datetime, and groups the data by contractID. The result
        is a dictionary keyed by contractID, with each value being a DataFrame of all snapshots
        for that contract, sorted by snapshot_date.
        
        Returns:
            Dict[str, pd.DataFrame]: Organized data keyed by contractID.
        """
        all_snapshots = []
        for snapshot_date, df in self.raw_data.items():
            df_copy = df.copy()
            df_copy["snapshot_date"] = pd.to_datetime(snapshot_date)
            all_snapshots.append(df_copy)
        if not all_snapshots:
            logger.warning("No raw data available to organize.")
            return {}
        combined_df = pd.concat(all_snapshots)
        combined_df['expiration'] = pd.to_datetime(combined_df['expiration'], errors='coerce')
        organized = {}
        for contract_id, group in combined_df.groupby("contractID"):
            organized[contract_id] = group.sort_values("snapshot_date")
        logger.info(f"Organized data for {len(organized)} contracts.")
        return organized

    def get_date_range_from_data(self, data_dict: Dict[str, pd.DataFrame]) -> (str, str):
        """
        Determines the full date range from the keys of the data dictionary.
        
        Returns:
            A tuple (min_date, max_date) in YYYYMMDD string format.
        """
        if not data_dict:
            return (self.initial_start_date.strftime("%Y%m%d"), self.initial_end_date.strftime("%Y%m%d"))
        dates = [pd.to_datetime(d) for d in data_dict.keys()]
        return (min(dates).strftime("%Y%m%d"), max(dates).strftime("%Y%m%d"))

    def get_default_filepath(self, processed: bool = True) -> str:
        """
        Generates a default file path for saving data.
        The file name is based on the symbol, function, and the full date range of the stored data.
        
        Args:
            processed (bool): If True, generates a path for processed data; else for raw data.
        
        Returns:
            str: The generated file path.
        """
        data_dict = self.processed_data if processed else self.raw_data
        start, end = self.get_date_range_from_data(data_dict)
        file_type = "processed" if processed else "raw"
        filename = f"{self.symbol}_{self.function}_{start}_{end}_{file_type}.pkl"
        return os.path.join(self.archive_dir, filename)

    def save_data(self, processed: bool = True, filepath: Optional[str] = None) -> None:
        """
        Saves the current data (raw or processed) to a pickle file.
        If no filepath is provided, a default file name is generated based on the current data's date range.
        
        Args:
            processed (bool): If True, saves processed_data; otherwise, raw_data.
            filepath (str, optional): File path to save the data.
        """
        if filepath is None:
            filepath = self.get_default_filepath(processed)
        data = self.processed_data if processed else self.raw_data
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)
            logger.info(f"Data saved to {filepath}.")
        except Exception as e:
            logger.error(f"Error saving data: {e}")

    def load_data(self, processed: bool = True, filepath: Optional[str] = None) -> None:
        """
        Loads data from a pickle file into processed_data or raw_data.
        
        Args:
            processed (bool): If True, loads into processed_data; otherwise, raw_data.
            filepath (str, optional): File path to load data from.
        """
        if filepath is None:
            filepath = self.get_default_filepath(processed)
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
            if processed:
                self.processed_data = data
            else:
                self.raw_data = data
            logger.info(f"Data loaded from {filepath}.")
        except Exception as e:
            logger.error(f"Error loading data: {e}")

    def update_saved_data(self, processed: bool = True) -> None:
        """
        Updates the saved data file by merging newly processed data with existing data.
        If a file already exists, it loads the existing data, merges it with the current data,
        and then saves the merged result back to the file. The file name will reflect the updated full date range.
        
        Args:
            processed (bool): If True, updates the processed data file; otherwise, raw data.
        """
        filepath = self.get_default_filepath(processed)
        existing_data = {}
        if os.path.exists(filepath):
            try:
                with open(filepath, 'rb') as f:
                    existing_data = pickle.load(f)
                logger.info(f"Existing data loaded from {filepath} for merging.")
            except Exception as e:
                logger.error(f"Error loading existing data from {filepath}: {e}")
        current_data = self.processed_data if processed else self.raw_data
        merged_data = {**existing_data, **current_data}
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(merged_data, f)
            logger.info(f"Merged data saved to {filepath}.")
        except Exception as e:
            logger.error(f"Error saving merged data to {filepath}: {e}")

    def get_data_slice(self, start: Optional[str] = None, end: Optional[str] = None, processed: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Retrieves a subset of the stored data snapshots based on snapshot dates.
        
        Args:
            start (str, optional): Start date (YYYY-MM-DD) for slicing.
            end (str, optional): End date (YYYY-MM-DD) for slicing.
            processed (bool): If True, returns processed_data; otherwise, raw_data.
        
        Returns:
            Dict[str, pd.DataFrame]: A dictionary of snapshots keyed by date.
        """
        data_source = self.processed_data if processed else self.raw_data
        sliced = {}
        for date_str, df in data_source.items():
            date_obj = pd.to_datetime(date_str)
            if start and date_obj < pd.to_datetime(start):
                continue
            if end and date_obj > pd.to_datetime(end):
                continue
            sliced[date_str] = df
        return sliced

if __name__ == "__main__":
    # Instantiate the processor with the test archive directory.
    processor = HistoricalOptionsDataProcessor(
        symbol="IBM",
        start_date="2022-01-01",
        end_date="2022-01-03",
        archive_dir=".test_processed_archive",
        freq='B'
    )
    
    # Fetch raw data.
    processor.process_data()
    
    # Save raw data.
    processor.save_data(processed=False)
    
    # Process raw data.
    processor.process_raw_data()
    
    # Save processed data.
    processor.save_data(processed=True)
    
    # Update the saved processed data by merging with current processed data.
    processor.update_saved_data(processed=True)
    
    # Organize processed data by contract.
    organized_contract_data = processor.organize_data_by_contract()
    for contract_id, df in organized_contract_data.items():
        logger.info(f"Contract {contract_id}: {len(df)} snapshots")
    
    # Load the processed data from file and print the head of each contract's DataFrame.
    processor.load_data(processed=True)
    organized_contract_data = processor.organize_data_by_contract()
    for contract_id, df in organized_contract_data.items():
        logger.info(f"Contract {contract_id} head:\n{df.head()}\n")
