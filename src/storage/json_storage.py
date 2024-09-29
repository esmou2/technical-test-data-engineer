import json
from pathlib import Path

from storage.storage import Storage
from logger.logger_config import logger

class JsonStorage(Storage):
    """Concrete class to save data as JSON files."""
    
    def __init__(self, storage_dir="data"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)  # if dir don't exist
        logger.info(f"JsonStorage initialized with directory: {self.storage_dir}")


    def save_data(self, data_type: str, data):
        """Save data as json file."""
        try:
            file_path = self.storage_dir / f"{data_type}.json"

            with open(file_path, "w") as f:
                json.dump(data, f, indent=4)
            logger.info(f"Data {data_type} saved to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {e}")