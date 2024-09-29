from pipeline.api_data_fetcher import APIDataFetcher
from storage.storage import Storage
from logger.logger_config import logger

class DataPipeline:
    def __init__(self, storage: Storage, fetcher: APIDataFetcher):
        self.data_storage = storage
        self.data_fetcher = fetcher


    def run(self):
        try:
            logger.info('Start pipeline')

            tracks_data = self.data_fetcher.fetch_data('tracks')
            users_data = self.data_fetcher.fetch_data('users')
            listen_history_data = self.data_fetcher.fetch_data('listen_history')

            logger.info('Save the data')

            tracks_data = self.data_storage.save_data('tracks',tracks_data, 'id')
            users_data = self.data_storage.save_data('users',users_data, 'id')
            listen_history_data = self.data_storage.save_data('listen_history',listen_history_data, 'user_id')

            logger.info('Pipeline executed successfully')
        except Exception as e:
            logger.error(f'An error occured while running the pipeline: {e}')
