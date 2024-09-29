import requests
from logger.logger_config import logger

class APIDataFetcher:
    def __init__(self, api_url='http://127.0.0.1:8000'):
        self.api_url = api_url
        logger.info(f"Initialized APIDataFetcher with base URL: {self.api_url}")

        

    def fetch_data(self, endpoint):
        url = f"{self.api_url}/{endpoint}"
        logger.info(f"Fetching data from {endpoint}")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully fetched {len(data)} items from {endpoint}")
            return 
        else:
            logger.error(f"Error fetching data from {url}: {response.status_code}")
            raise Exception(f"Error fetching data from {endpoint}: {response.status_code}")
        