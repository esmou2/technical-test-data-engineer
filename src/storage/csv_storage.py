from pathlib import Path
import os
import pandas as pd
from datetime import datetime
from pipeline.data_category import DataCategory
from storage.storage import Storage
from logger.logger_config import logger
from typing import Union

class CSVStorage(Storage):
    """
    Class to handle saving and loading data from a CSV file.

    Attributes:
        storage_dir (Path): The directory where CSV files are saved.
    """
    
    def __init__(self, storage_dir: str = "data"):
        """
        Initializes the CSVStorage with the specified directory.
        If the directory does not exist, it creates it.

        Args:
            storage_dir (str): The directory path where CSV files will be stored.

        Raises:
            OSError: If the directory creation fails.
        """
        try:
            self.storage_dir = Path(storage_dir)
            self.storage_dir.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
            logger.info(f"CSVStorage initialized with directory: {self.storage_dir}")
        except OSError as e:
            logger.error(f"Failed to create or access storage directory: {self.storage_dir}. Error: {e}")
            raise
    
    def save_data(self, category: Union[str, DataCategory], data_df: pd.DataFrame, unique_key: str) -> None:
        """
        Saves the given DataFrame to a CSV file, updating or inserting records as needed.

        - If records with the same unique key exist, they are updated.
        - If new records are found, they are inserted.
        - Adds a `charged_at` timestamp to indicate when the record was processed.

        Args:
            category (Union[str, DataCategory]): The category of the data (used for file naming).
            data_df (pd.DataFrame): The DataFrame containing data to be saved.
            unique_key (str): The field used to uniquely identify records for updates.

        Raises:
            ValueError: If the DataFrame is empty or missing required fields.
            Exception: If saving the data fails for any reason.
        """
        if data_df.empty:
            raise ValueError("The data DataFrame is empty, nothing to save.")

        try:
            category_str = category.value if hasattr(category, 'value') else str(category)

            current_datetime = datetime.now().strftime("%Y-%m-%dT%H:%M")
            file_path = os.path.join(self.storage_dir, f"{category_str}.csv")

            existing_data = self.load_existing_data(file_path)

            if not existing_data.empty and 'charged_at' in existing_data.columns:
                existing_data['charged_at'] = pd.to_datetime(existing_data['charged_at'])
                max_charged_at = existing_data['charged_at'].max()

                if 'created_at' in data_df.columns and 'updated_at' in data_df.columns:
                    data_df['created_at'] = pd.to_datetime(data_df['created_at'])
                    data_df['updated_at'] = pd.to_datetime(data_df['updated_at'])

                    new_data_df = data_df[data_df['created_at'] > max_charged_at]
                    updated_data_df = data_df[data_df['updated_at'] > max_charged_at]

                    if new_data_df.empty and updated_data_df.empty:
                        logger.info(f"No new or updated records to save for {category_str}.")
                        return

                    # Update existing
                    if not updated_data_df.empty:
                        for _, updated_row in updated_data_df.iterrows():
                            existing_data.loc[existing_data[unique_key] == updated_row[unique_key]] = updated_row
                        logger.info(f"Updated {len(updated_data_df)} records in {category_str}.csv")

                    # Insert new
                    if not new_data_df.empty:
                        new_data_df['charged_at'] = current_datetime
                        existing_data = pd.concat([existing_data, new_data_df], ignore_index=True)
                        logger.info(f"Inserted {len(new_data_df)} new records into {category_str}.csv")
                else:
                    raise ValueError("Data is missing 'created_at' or 'updated_at' fields.")
            else:
                data_df['charged_at'] = current_datetime
                existing_data = data_df
                logger.info(f"Inserted {len(existing_data)} new records into {category_str}.csv")
            
            existing_data = existing_data.drop(columns=['created_at', 'updated_at'], errors='ignore')
            existing_data.to_csv(file_path, mode='w', index=False, header=True)
            logger.info(f"Data successfully saved to {file_path}")

        except ValueError as e:
            logger.error(f"Data validation error for {category_str}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {e}")
            raise
    
    def load_existing_data(self, file_path: str) -> pd.DataFrame:
            """
            Loads existing data from a CSV file, or returns an empty DataFrame if the file doesn't exist.

            Args:
                file_path (str): The path to the CSV file.

            Returns:
                pd.DataFrame: The loaded data as a DataFrame, or an empty DataFrame if the file doesn't exist.
            """
            if os.path.exists(file_path):
                return pd.read_csv(file_path)
            else:
                logger.info(f"No existing data found at {file_path}. Returning an empty DataFrame.")
                return pd.DataFrame()

