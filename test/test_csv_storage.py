import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from storage.csv_storage import CSVStorage
from pipeline.data_category import DataCategory

import os
@pytest.fixture
def setup_csv_storage(tmp_path):
    """
    Fixture to initialize CSVStorage with a temporary directory.
    """
    storage_dir = tmp_path / "test_data"
    storage = CSVStorage(storage_dir=str(storage_dir))
    return storage, storage_dir


def test_init_creates_directory(setup_csv_storage):
    """
    Test that the CSVStorage initializes and creates the storage directory.
    """
    storage, storage_dir = setup_csv_storage
    assert Path(storage_dir).exists()


def test_save_data_empty_df(setup_csv_storage):
    """
    Test that saving an empty DataFrame raises a ValueError.
    """
    storage, _ = setup_csv_storage

    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="The data DataFrame is empty, nothing to save."):
        storage.save_data(DataCategory.TRACKS, empty_df, 'id')


@patch('pandas.DataFrame.to_csv')
def test_save_new_data(mock_to_csv, setup_csv_storage):
    """
    Test saving new data to a CSV file.
    """
    storage, _ = setup_csv_storage

    mock_data = pd.DataFrame({
        "id": [1, 2],
        "name": ["Track1", "Track2"],
        "created_at": pd.to_datetime(["2024-09-28", "2024-09-29"]),
        "updated_at": pd.to_datetime(["2024-09-28", "2024-09-29"])
    })

    storage.save_data(DataCategory.TRACKS, mock_data, "id")

    mock_to_csv.assert_called_once()
    args, kwargs = mock_to_csv.call_args
    assert kwargs["index"] is False
    assert kwargs["header"] is True


def test_load_existing_data_with_file(setup_csv_storage):
    """
    Test that CSVStorage.load_existing_data correctly loads data from an existing file using tmp_path.
    """
    storage, storage_dir = setup_csv_storage
    file_path = storage_dir / f"{DataCategory.TRACKS.value}.csv"

    mock_data = pd.DataFrame({
        "id": [1, 2],
        "name": ["Track1", "Track2"]
    })
    mock_data.to_csv(file_path, index=False)

    loaded_data = storage.load_existing_data(file_path)

    assert not loaded_data.empty
    assert loaded_data.equals(mock_data)

def test_load_existing_data_no_file(setup_csv_storage):
    """
    Test that loading data from a non-existent file returns an empty DataFrame.
    """
    storage, _ = setup_csv_storage

    data = storage.load_existing_data("non_existent_file.csv")

    assert data.empty
