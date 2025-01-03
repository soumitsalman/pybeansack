from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class User(BaseModel):
    id: Optional[str] = Field(default=None, serialization_alias="_id")  
    email: str = None 
    name: Optional[str] = None
    image_url: Optional[str] = None  
    linked_accounts: Optional[list[str]] = None
    following: Optional[list[str]] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None

class Barista(BaseModel):
    id: str = Field(default=None, alias="_id")
    owner: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    urls: Optional[list[str]] = None
    query: Optional[str] = None
    embedding: Optional[list[float]] = None
    accuracy: Optional[float] = None
    tags: Optional[list[str]] = None
    kinds: Optional[list[str]] = None
    sources: Optional[list[str]] = None
    last_ndays: Optional[int] = None
    created: Optional[datetime] = None