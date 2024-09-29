from pathlib import Path
import os
from storage.storage import Storage
from logger.logger_config import logger
import pandas as pd

class CSVStorage(Storage):
    """class to save data in a CSV."""
    
    def __init__(self, storage_dir="data"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)  # if dir don't exist
        logger.info(f"CSVStorage initialized with directory: {self.storage_dir}")


    def save_data(self, data_type, data, unique_key):
        """Save data to csv file."""
        try:
            file_path = os.path.join(self.storage_dir, f"{data_type}.csv")
            existing_data = self.load_existing_data(file_path)
            data_df = pd.DataFrame(data)

            if not existing_data.empty:
                data_df = data_df[~data_df[unique_key].isin(existing_data[unique_key])]
            
            if not data_df.empty:
                data_df.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path))
                print(f"Saved {len(data_df)} new records to {data_type}.csv")
            else:
                print(f"Saved {len(data_df)} new records to {data_type}.csv")

        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {e}")

    
    def load_existing_data(self, file_path):
        """Load existing data from a CSV file, or return an empty DataFrame if the file doesn't exist."""
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        else:
            return pd.DataFrame() 