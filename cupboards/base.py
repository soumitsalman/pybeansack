import datetime
import logging
import os
from abc import ABC, abstractmethod
from typing import Literal, Any

K_ID = 'id'
K_BASE_URL = 'base_url'
K_URL = 'url'
K_RELATED_URL = 'related_url'
K_EMBEDDING = 'embedding'

DATA_TYPES = Literal["events", "signals", "sources"]

VECTOR_LEN = int(os.getenv('VECTOR_LEN', 384))

log = logging.getLogger("cupboard")

class CupboardBase(ABC):
    @abstractmethod
    async def __aenter__(self):
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc, tb):
        pass

    @abstractmethod
    async def store(self, data_type: DATA_TYPES, items: list[dict[str, Any]]):
        pass

    @abstractmethod
    async def link_events(self, links: list[dict[str, str]]):
        pass
    
    @abstractmethod
    async def query_events(
        self,
        created: datetime = None,
        tags: list[str] = None,
        embedding: list[float] = None, distance: float = None,
        conditions: dict[str, Any] | list[str] = None,
        limit: int = None,
        columns: list[str] | None = None,
    ):
        pass    
    
    def optimize(self):
        pass
 