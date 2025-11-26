from abc import ABC, abstractmethod
from .models import *

NOT_IMPLEMENTED = NotImplementedError("Method not implemented in base class")

class BeansackBase(ABC):
    @abstractmethod
    def deduplicate(self, table: str, items: list) -> list:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def store_beans(self, beans: list[Bean]) -> int:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def store_chatters(self, chatters: list[Chatter]) -> int:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def store_publishers(self, publishers: list[Publisher]) -> int:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def update_beans(self, beans: list[Bean], columns: list[str]) -> int:
        raise NOT_IMPLEMENTED

    @abstractmethod
    def update_embeddings(self, beans: list[Bean]) -> int:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def update_publishers(self, publishers: list[Publisher]) -> int:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def query_latest_beans(self, 
        kind: str = None, 
        created: datetime = None, 
        collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None) -> list[Bean]:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def query_trending_beans(self,
        kind: str = None, 
        updated: datetime = None, 
        collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[AggregatedBean]:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def query_aggregated_beans(self,
        kind: str = None, 
        created: datetime = None, 
        collected: datetime = None,
        updated: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[AggregatedBean]:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def query_aggregated_chatters(self, urls: list[str] = None, updated: datetime = None, limit: int = 0, offset: int = 0):        
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def query_publishers(self, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0) -> list[Publisher]:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def count_rows(self, table: str, conditions: list[str] = None) -> int:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def refresh():
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def close():
        raise NOT_IMPLEMENTED

class CupboardBase(ABC):
    @abstractmethod
    def store_mugs(self, mugs: list[Mug]) -> int:
        raise NOT_IMPLEMENTED
    
    @abstractmethod
    def store_sips(self, sips: list[Sip]) -> int:
        raise NOT_IMPLEMENTED