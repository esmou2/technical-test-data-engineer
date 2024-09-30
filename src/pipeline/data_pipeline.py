import pandas as pd
from typing import List, Dict, Any
from pipeline.api_data_fetcher_async import APIDataFetcherAsync
from pipeline.data_category import DataCategory
from storage.storage import Storage
from logger.logger_config import logger

class DataPipeline:
    """
    A data pipeline class responsible for fetching data asynchronously
    from multiple API endpoints and saving it into a storage system.

    Attributes:
        data_storage (Storage): An instance of the storage handler for saving data.
        data_fetcher (APIDataFetcherAsync): An asynchronous data fetcher to retrieve data from APIs.
    """
    
    def __init__(self, storage: Storage, fetcher: APIDataFetcherAsync):
        """
        Initializes the DataPipeline with the required storage and data fetcher.

        Args:
            storage (Storage): A storage instance to handle data persistence.
            fetcher (APIDataFetcherAsync): An API data fetcher instance to retrieve data asynchronously.
        """
        self.data_storage = storage
        self.data_fetcher = fetcher  
    
    def clean_data(self, data: List[Dict[str, Any]], key_field: str) -> pd.DataFrame:
        """
        Cleans and validates data using a DataFrame. This method:
        - Removes rows with missing required fields.
        - Removes duplicates based on the 'id' field.

        Args:
            data (list): The list of data to clean.
            required_fields (list): The list of fields that must be present in each record.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """        

        df = pd.DataFrame(data)
        df_cleaned = df.dropna(how='any')
        df_cleaned = df_cleaned.drop_duplicates(subset=[key_field])

        return df_cleaned

    async def fetch_and_save(self, category: DataCategory, key_field: str) -> None:
        """
        Fetches data for a given category and saves it using the provided key field.

        Args:
            category (str): The category of data to fetch (e.g., 'tracks', 'users', 'listen_history').
            key_field (str): The key field used for saving the data (e.g., 'id', 'user_id').

        Raises:
            Exception: Propagates any exceptions encountered during fetching or saving.
        """
        try:
            logger.info(f'Fetching data for {category.value}')
            data = await self.data_fetcher.fetch_all_data(category.value)
            logger.info(f'No data for {category.value}')
            if data:
                logger.info(f'Cleaning data for {category.value}')
                cleaned_data_df = self.clean_data(data, key_field)
                logger.info(f'Saving data for {category.value}')
                self.data_storage.save_data(category, cleaned_data_df, key_field)
        except Exception as e:
            logger.error(f'Failed to fetch and save data for {category.value}: {e}')
            raise e

    async def run(self) -> None:
        """
        Executes the data pipeline by fetching data from multiple sources
        asynchronously and saving it to the specified storage.

        The pipeline fetches data for 'tracks', 'users', and 'listen_history',
        then stores each dataset using the corresponding key field ('id' or 'user_id').

        Logs the start, completion, and any errors encountered during execution.
        """
        try:
            logger.info('Start pipeline')

            await self.fetch_and_save(DataCategory.TRACKS, 'id')
            await self.fetch_and_save(DataCategory.USERS, 'id')
            await self.fetch_and_save(DataCategory.LISTEN_HISTORY, 'user_id')

            logger.info('Pipeline executed successfully')
        except Exception as e:
            logger.error(f'An error occurred while running the pipeline: {e}')


