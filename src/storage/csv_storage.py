from pathlib import Path
import os
from storage.storage import Storage
from logger.logger_config import logger
import pandas as pd
from datetime import datetime, timedelta

class CSVStorage(Storage):
    """class to save data in a CSV."""
    
    def __init__(self, storage_dir="data"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)  # if dir don't exist
        logger.info(f"CSVStorage initialized with directory: {self.storage_dir}")


    def save_data(self, data_type, data, unique_key):
        """Save data to csv file."""
        try:
            current_datetime = datetime.now().strftime("%Y-%m-%dT%H:%M")
            file_path = os.path.join(self.storage_dir, f"{data_type}.csv")
            
            data_df = pd.DataFrame(data)
            existing_data = self.load_existing_data(file_path)


            if not existing_data.empty and 'charged_at' in existing_data.columns:
                existing_data['charged_at'] = pd.to_datetime(existing_data['charged_at'])
                max_charged_at = existing_data['charged_at'].max()
                logger.info(f"Max charged_at: {max_charged_at}")

                data_df['created_at'] = pd.to_datetime(data_df['created_at'])
                data_df['updated_at'] = pd.to_datetime(data_df['updated_at'])

                new_data_df = data_df[data_df['created_at'] > max_charged_at]
                updated_data_df = data_df[data_df['updated_at'] > max_charged_at]

                if not updated_data_df.empty:
                    for _, updated_row in updated_data_df.iterrows():
                        existing_data.loc[existing_data[unique_key] == updated_row[unique_key]] = updated_row
                    logger.info(f"Updated {len(updated_data_df)} records in {data_type}.csv")

                if not new_data_df.empty:
                    new_data_df['charged_at'] = current_datetime
                    existing_data = pd.concat([existing_data, new_data_df], ignore_index=True)
                    logger.info(f"Inserted {len(new_data_df)} new records into {data_type}.csv")

            else:
                data_df['charged_at'] = current_datetime
                existing_data = data_df
                logger.info(f"Inserted {len(existing_data)} new records into {data_type}.csv")

            existing_data.to_csv(file_path, mode='w', index=False, header=True)
            logger.info(f"Data successfully saved to {file_path}")

        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {e}")

    
    def load_existing_data(self, file_path):
        """Load existing data from a CSV file, or return an empty DataFrame if the file doesn't exist."""
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        else:
            return pd.DataFrame()
        