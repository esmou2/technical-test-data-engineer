from enum import Enum

class DataCategory(Enum):
    """
    Enum for representing different data categories to fetch from the API.
    
    Attributes:
        TRACKS: Represents the 'tracks' category.
        USERS: Represents the 'users' category.
        LISTEN_HISTORY: Represents the 'listen_history' category.
    """
    
    TRACKS = 'tracks'
    USERS = 'users'
    LISTEN_HISTORY = 'listen_history'