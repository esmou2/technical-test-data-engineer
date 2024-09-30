import aiohttp
from logger.logger_config import logger
from typing import List

class APIDataFetcherAsync:
    """
    A class to asynchronously fetch paginated data from a given API endpoint.

    Attributes:
        api_url (str): The base URL of the API.
        page_size (int): The number of items to fetch per page.
    """

    def __init__(self, api_url: str = 'http://127.0.0.1:8000', page_size: int = 100):
        """
        Initializes the APIDataFetcherAsync with the provided API URL and page size.

        Args:
            api_url (str): The base URL of the API.
            page_size (int): The number of items to fetch per page.
        """
        self.api_url = api_url
        self.page_size = page_size
        logger.info(f"Initialized APIDataFetcher with base URL: {self.api_url}")

    async def fetch_all_data(self, endpoint: str) -> List[dict]:
        """
        Fetch all pages of data asynchronously from the specified endpoint.

        This method fetches paginated data from the API until no more data is available.

        Args:
            endpoint (str): The API endpoint to fetch data from.

        Returns:
            List[dict]: A list of all the fetched data items.
        """
        all_data = []
        page = 1
        
        async with aiohttp.ClientSession() as session:
            while True:
                page_data = await self._fetch_page(session, endpoint, page)
                if not page_data:
                    break
                all_data.extend(page_data)
                page += 1 
        
        logger.info(f"Total of {len(all_data)} items fetched from {endpoint}")
        return all_data
    
    async def _fetch_page(self, session: aiohttp.ClientSession, endpoint: str, page: int) -> List[dict]:
        """
        Fetch a single page of data asynchronously from the specified endpoint.

        Args:
            session (aiohttp.ClientSession): The aiohttp session used for the HTTP request.
            endpoint (str): The API endpoint to fetch data from.
            page (int): The page number to fetch.

        Returns:
            List[dict]: A list of items from the page. If the request fails or no data is found, an empty list is returned.
        """
        url = f"{self.api_url}/{endpoint}?page={page}&size={self.page_size}"
        try:
            async with session.get(url) as response:
                # Raise an HTTP exception if the status code is not 200-299
                response.raise_for_status()
                data = await response.json()

                # Ensure 'items' exists in the response, or handle missing keys gracefully
                items = data.get("items", [])
                return items

        except aiohttp.ClientResponseError as e:
            # Specific exception handling for HTTP response errors
            logger.error(f"HTTP error occurred while fetching data from {url}: {e}")
        except aiohttp.ClientConnectionError as e:
            # Handle connection issues
            logger.error(f"Connection error occurred while accessing {url}: {e}")
        except aiohttp.ClientPayloadError as e:
            # Handle payload/response format issues
            logger.error(f"Payload error occurred while parsing response from {url}: {e}")
        except Exception as e:
            # General exception handling for any other issues
            logger.error(f"Unexpected error occurred while fetching data from {url}: {e}")

        return []
