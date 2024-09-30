from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pytest_asyncio
import pandas as pd
from pipeline.data_pipeline import DataPipeline
from pipeline.data_category import DataCategory

from pandas.testing import assert_frame_equal


MOCK_TRACKS = [
    {
        "id": 1,
        "name": "outside",
        "artist": "Adrian Cook",
        "songwriters": "Christopher Williams",
        "duration": "33:34",
        "genres": "line",
        "album": "among"
    }
]

USER_MOCK = [
    {
        "id": 1,
        "first_name": "Michelle",
        "last_name": "Taylor"
    }
]

LISTEN_HISTORY_MOCK = [
    {
        "user_id": 1,
        "items": [1]
    }
]

@pytest_asyncio.fixture
async def setup_pipeline():
    """
    Fixture to set up the mock storage and fetcher for the pipeline.
    """
    mock_storage = MagicMock()
    mock_fetcher = AsyncMock()

    # Initialize DataPipeline with the mock objects
    pipeline = DataPipeline(storage=mock_storage, fetcher=mock_fetcher)
    return pipeline, mock_storage, mock_fetcher

@pytest.mark.asyncio
@patch.object(DataPipeline, 'clean_data')  # Mock the clean_data method
async def test_fetch_and_save_success(mock_clean_data, setup_pipeline):
    """
    Test that fetch_and_save successfully fetches and saves data.
    """
    pipeline, mock_storage, mock_fetcher = setup_pipeline
    mock_fetcher.fetch_all_data.return_value = MOCK_TRACKS

    mock_clean_data.return_value = pd.DataFrame(MOCK_TRACKS)

    await pipeline.fetch_and_save(DataCategory.TRACKS, 'id')

    mock_fetcher.fetch_all_data.assert_called_once_with(DataCategory.TRACKS.value)
    mock_clean_data.assert_called_once_with(MOCK_TRACKS, 'id')
    mock_storage.save_data.assert_called_once_with(DataCategory.TRACKS, mock_clean_data.return_value, 'id')


@pytest.mark.asyncio
async def test_fetch_and_save_failure(setup_pipeline):
    """
    Test that fetch_and_save handles an exception during data fetching.
    """
    pipeline, mock_storage, mock_fetcher = setup_pipeline
    mock_fetcher.fetch_all_data.side_effect = Exception('Fetch error')

    with pytest.raises(Exception, match='Fetch error'):
        await pipeline.fetch_and_save(DataCategory.TRACKS, 'id')

    mock_storage.save_data.assert_not_called()

@pytest.mark.asyncio
async def test_fetch_and_save_empty_data(setup_pipeline):
    """
    Test that fetch_and_save handles empty data correctly.
    """
    pipeline, mock_storage, mock_fetcher = setup_pipeline
    mock_fetcher.fetch_all_data.return_value = []

    await pipeline.fetch_and_save(DataCategory.TRACKS, 'id')

    mock_fetcher.fetch_all_data.assert_called_once_with(DataCategory.TRACKS.value)
    
    mock_storage.save_data.assert_not_called()


@pytest.mark.asyncio
@patch.object(DataPipeline, 'clean_data')
async def test_run_partial_failure(mock_clean_data, setup_pipeline):
    """
    Test that the run method continues to fetch and save other data even if one category fails.
    """
    pipeline, mock_storage, mock_fetcher = setup_pipeline

    mock_fetcher.fetch_all_data.side_effect = [
        MOCK_TRACKS, 
        [], 
        LISTEN_HISTORY_MOCK
    ]

    mock_clean_data.side_effect = [
        pd.DataFrame(MOCK_TRACKS),
        pd.DataFrame(LISTEN_HISTORY_MOCK) 
    ]

    await pipeline.run()

    assert mock_fetcher.fetch_all_data.call_count == 3

    mock_clean_data.assert_any_call(MOCK_TRACKS, 'id')
    mock_clean_data.assert_any_call(LISTEN_HISTORY_MOCK, 'user_id')

    assert mock_storage.save_data.call_count == 2

@pytest.mark.asyncio
@patch.object(DataPipeline, 'clean_data')
async def test_clean_data_with_missing_fields(mock_clean_data, setup_pipeline):
    """
    Test that clean_data filters out rows with missing or invalid fields.
    """
    pipeline, mock_storage, mock_fetcher = setup_pipeline

    invalid_data = [
        {"id": 1, "name": "Track1"},
        {"id": 2, "name": None},
        {"id": None, "name": "Track3"},
    ]

    mock_fetcher.fetch_all_data.return_value = invalid_data

    cleaned_data_df = pd.DataFrame([
        {"id": 1, "name": "Track1"}
    ])

    mock_clean_data.return_value = cleaned_data_df

    await pipeline.fetch_and_save(DataCategory.TRACKS, 'id')

    mock_clean_data.assert_called_once_with(invalid_data, 'id')
    mock_storage.save_data.assert_called_once_with(DataCategory.TRACKS, cleaned_data_df, 'id')

@pytest.mark.asyncio
@patch.object(DataPipeline, 'clean_data')
async def test_clean_data_with_duplicates(mock_clean_data, setup_pipeline):
    """
    Test that clean_data filters out rows with missing or invalid fields.
    """
    pipeline, mock_storage, mock_fetcher = setup_pipeline

    invalid_data = [
        {"id": 1, "name": "Track1"},
        {"id": 1, "name": "Track1"},
        {"id": 3, "name": "Track3"},
    ]

    mock_fetcher.fetch_all_data.return_value = invalid_data

    cleaned_data_df = pd.DataFrame([
        {"id": 3, "name": "Track3"}
    ])

    mock_clean_data.return_value = cleaned_data_df

    await pipeline.fetch_and_save(DataCategory.TRACKS, 'id')

    mock_clean_data.assert_called_once_with(invalid_data, 'id')
    mock_storage.save_data.assert_called_once_with(DataCategory.TRACKS, cleaned_data_df, 'id')