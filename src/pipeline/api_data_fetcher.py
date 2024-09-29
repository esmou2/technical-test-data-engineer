import requests
from logger.logger_config import logger

class APIDataFetcher:
    def __init__(self, api_url='http://127.0.0.1:8000', page_size=100):
        self.api_url = api_url
        self.page_size = page_size
        logger.info(f"Initialized APIDataFetcher with base URL: {self.api_url}")

        

    def fetch_data(self, endpoint):
        logger.info(f"Fetching data from {endpoint}")
        all_data = []
        page = 1
        while True:
            url = f"{self.api_url}/{endpoint}?page={page}&size={self.page_size}"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json().get("items", [])
                if not data:
                    break
                all_data.extend(data)
                page += 1
            else:
                logger.error(f"Error fetching data from {url}: {response.status_code}")
                raise Exception(f"Error fetching data from {endpoint}: {response.status_code}")
        logger.info(f"Successfully fetched {len(all_data)} items from {self.api_url}/{endpoint}")
        return all_data
            
        