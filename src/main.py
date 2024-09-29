from pipeline.api_data_fetcher import APIDataFetcher
from pipeline.data_pipeline import DataPipeline
from storage.csv_storage import CSVStorage


if __name__ == "__main__":
    print('test')
    # init the DataFetcher
    fetcher = APIDataFetcher()
    # init the json storage
    csv_storage = CSVStorage()
    pipeline = DataPipeline(storage=csv_storage, fetcher=fetcher)
    pipeline.run()
