import pytest
import aiohttp
from aioresponses import aioresponses

from pipeline.api_data_fetcher_async import APIDataFetcherAsync

USER_MOCK = {"items": [
    {
        "id": 1,
        "first_name": "Michelle",
        "last_name": "Taylor"
    },
    {
        "id": 2,
        "first_name": "Peggy",
        "last_name": "Brooks"
    }
]}

USER_MOCK_2 = {"items": [
    {
        "id": 1,
        "first_name": "Michelle",
        "last_name": "Taylor"
    },
    {
        "id": 2,
        "first_name": "Peggy",
        "last_name": "Brooks"
    }
]}

API_URL ="http://testserver"
ENDPOINT = "users"
PAGE_1 = 1
PAGE_SIZE = 2

@pytest.mark.asyncio
async def test_fetch_all_data_success():
    fetcher = APIDataFetcherAsync(api_url=API_URL, page_size=PAGE_SIZE)

    with aioresponses() as mocked:
        
        url = f"{API_URL}/{ENDPOINT}?page={PAGE_1}&size={PAGE_SIZE}"
        mocked.get(url, payload=USER_MOCK)

        async with aiohttp.ClientSession() as session:
            result = await fetcher.fetch_all_data(ENDPOINT)
            
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2

@pytest.mark.asyncio
async def test_fetch_all_data_failure():
    fetcher = APIDataFetcherAsync(api_url=API_URL, page_size=PAGE_SIZE)

    with aioresponses() as mocked:
        url = f"{API_URL}/{ENDPOINT}?page={PAGE_1}&size={PAGE_SIZE}"
        
        mocked.get(url, status=500)

        async with aiohttp.ClientSession() as session:
            result = await fetcher.fetch_all_data(ENDPOINT)
            
    assert result == []

@pytest.mark.asyncio
async def test_fetch_all_data_pagination():
    fetcher = APIDataFetcherAsync(api_url=API_URL, page_size=PAGE_SIZE)

    with aioresponses() as mocked:
        url_page_1 = f"{API_URL}/{ENDPOINT}?page=1&size={PAGE_SIZE}"
        url_page_2 = f"{API_URL}/{ENDPOINT}?page=2&size={PAGE_SIZE}"
        url_page_3 = f"{API_URL}/{ENDPOINT}?page=3&size={PAGE_SIZE}"

        
        mocked.get(url_page_1, payload=USER_MOCK)
        mocked.get(url_page_2, payload=USER_MOCK_2)
        mocked.get(url_page_3, payload={"items": []})

        async with aiohttp.ClientSession() as session:
            result = await fetcher.fetch_all_data(ENDPOINT)

    assert len(result) == 4
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2

@pytest.mark.asyncio
async def test_fetch_all_data_exception_handling():
    fetcher = APIDataFetcherAsync(api_url=API_URL, page_size=PAGE_SIZE)

    with aioresponses() as mocked:
        url = f"{API_URL}/{ENDPOINT}?page={PAGE_1}&size={PAGE_SIZE}"
        mocked.get(url, exception=aiohttp.ClientError)

        async with aiohttp.ClientSession() as session:
            result = await fetcher.fetch_all_data(ENDPOINT)

    assert result == []

@pytest.mark.asyncio
async def test_fetch_all_data_empty_response():
    fetcher = APIDataFetcherAsync(api_url=API_URL, page_size=PAGE_SIZE)

    empty_response = {"items": []}

    with aioresponses() as mocked:
        url = f"{API_URL}/{ENDPOINT}?page={PAGE_1}&size={PAGE_SIZE}"
        mocked.get(url, payload=empty_response)

        async with aiohttp.ClientSession() as session:
            result = await fetcher.fetch_all_data(ENDPOINT)

    assert result == []