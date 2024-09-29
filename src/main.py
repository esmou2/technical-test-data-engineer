from pipeline.api_data_fetcher import APIDataFetcher
from pipeline.data_pipeline import DataPipeline
from storage.json_storage import JsonStorage


if __name__ == "__main__":
    print('test')
    # init the DataFetcher
    fetcher = APIDataFetcher()
    # init the json storage
    json_storage = JsonStorage()
    pipeline = DataPipeline(storage=json_storage, fetcher=fetcher)
    pipeline.run()
