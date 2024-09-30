import asyncio
import time
import os
from pipeline.api_data_fetcher_async import APIDataFetcherAsync
from pipeline.data_pipeline import DataPipeline
from storage.csv_storage import CSVStorage
from logger.logger_config import logger
from dotenv import load_dotenv

load_dotenv()

async def main():
    """
    Main function to initialize the pipeline and run the data fetching and saving process.
    The function also measures execution time and logs it.
    """
    start_time = time.time()
    try:
        logger.info("Starting the data pipeline execution...")

        api_url = os.getenv("API_URL", "http://127.0.0.1:8000")
        fetcher = APIDataFetcherAsync(api_url=api_url)

        storage_dir = os.getenv("STORAGE_DIR", "data")
        csv_storage = CSVStorage(storage_dir=storage_dir)

        pipeline = DataPipeline(storage=csv_storage, fetcher=fetcher)

        await pipeline.run()

        # Measure and log the execution time
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Data pipeline execution completed in {execution_time:.2f} seconds.")

    except Exception as e:
        logger.error(f"An error occurred during the data pipeline execution: {e}")
    finally:
        logger.info("Data pipeline execution finished.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Pipeline execution interrupted by user.")


