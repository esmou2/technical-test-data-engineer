from abc import ABC, abstractmethod

class Storage(ABC):
    """Interface for data storage."""
    
    @abstractmethod
    def save_data(self, data_type, data):
        """Save data to the storage."""
        pass