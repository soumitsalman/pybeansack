from rfc3339 import rfc3339
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional
from datetime import datetime
from .utils import *

# CHANNEL = "social media group/forum"
POST = "post"
JOB = "job"
NEWS = "news"
BLOG = "blog"
COMMENTS = "comments"
OPED = "opinion"

# names of important fields of collections
K_ID="_id"
K_URL="url"
K_KIND = "kind"
K_CATEGORIES = "categories"
K_SENTIMENTS = "sentiments"
K_TAGS = "tags"
K_TITLE = "title"
K_CONTENT = "content"
K_SOURCE = "source"
K_CHATTER_GROUP = "group"
K_EMBEDDING = "embedding"
K_GIST = "gist"
K_SUMMARY = "summary"
K_REGIONS = "regions"
K_ENTITIES = "entities"
K_UPDATED = "updated"
K_COLLECTED = "collected"
K_CLUSTER_ID = "cluster_id"
K_HIGHLIGHTS = "highlights"
K_IMAGEURL = "image_url"
K_CREATED = "created"
K_AUTHOR = "author"
K_SEARCH_SCORE = "search_score"
K_RELATED = "related"
K_LATEST_LIKES = "latest_likes"
K_LATEST_COMMENTS = "latest_comments"
K_LATEST_SHARES = "latest_shares"
K_SHARED_IN = "shared_in"
K_TRENDSCORE = "trend_score"
K_CHATTER_URL = "chatter_url"
K_LIKES = "likes"
K_COMMENTS = "comments"
K_SHARES = "shares"
K_OWNER = "owner"
K_FOLLOWING = "following"
K_DESCRIPTION = "description"

K_RESTRICTED_CONTENT = "restricted_content"
K_CONTENT_LENGTH = "content_length"
SUMMARY_LENGTH = "summary_length"
TITLE_LENGTH = "title_length"

# K_SITE_NAME = "site_name"
K_BASE_URL = "base_url"
K_RSS_FEED = "rss_feed"
K_FAVICON = "favicon"

SYSTEM = "__SYSTEM__"

class Chatter(BaseModel):
    chatter_url: Optional[str] = Field(default=None, min_length=1) # this is the url of the social media post that contains the Bean url
    url: str = Field(min_length=1) # this the url from Bean
    source: Optional[str] = Field(default=None) # this is the domain name of the source
    forum: Optional[str] = Field(default=None) # this is the group/forum the chatter was collected from
    collected: Optional[datetime] = Field(default=None)
    likes: int = Field(default=0)
    comments: int = Field(default=0)
    shares: int = Field(default=0)
    subscribers: int = Field(default=0)

    def to_tuple(self) -> tuple:
        return (
            self.chatter_url,
            self.url,
            self.source,
            self.forum,
            self.collected,
            self.likes,
            self.comments,
            self.subscribers
        )

    class Config:
        json_encoders={datetime: rfc3339}
        dtype_specs = {
            'chatter_url': 'string',
            'url': 'string',
            'source': 'string',
            'forum': 'string',
            'likes': 'uint32',
            'comments': 'uint32',
            'shares': 'uint32',
            'subscribers': 'uint32'
        }

class Publisher(BaseModel):
    source: str = Field(min_length=1) # this is domain name that gets matched with the source field in Bean
    base_url: str = Field(min_length=1)
    title: Optional[str] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    favicon: Optional[str] = Field(default=None)
    rss_feed: Optional[str] = Field(default=None)

    class Config:
        dtype_specs = {
            'source': 'string',
            'base_url': 'string',
            'title': 'string',
            'summary': 'string',
            'favicon': 'string',
            'rss_feed': 'string'
        }

class Bean(BaseModel):
    # collected / scraped fields
    id: str = Field(default=None, alias="_id")
    url: str    
    kind: Optional[str] = None
    source: Optional[str] = None
    title: Optional[str] = None
    title_length: Optional[int] = None
    summary: Optional[str] = None
    summary_length: Optional[int] = None
    content: Optional[str] = None
    content_length: Optional[int] = None
    restricted_content: Optional[bool] = None
    image_url: Optional[str] = None
    author: Optional[str] = None    
    created: Optional[datetime] = None 
    collected: Optional[datetime] = None

    # generated fields
    embedding: Optional[list[float]] = None
    gist: Optional[str] = None

    # computed fields
    cluster_id: Optional[str] = None
    cluster_size: Optional[int] = Field(default=0)
    related: Optional[list[str]] = Field(default=None)    
    categories: Optional[list[str]] = None
    entities: Optional[list[str]] = None
    regions: Optional[list[str]] = None
    sentiments: Optional[list[str]] = None
    tags: Optional[list[str]|str] = None
    
    # social media stats
    publisher: Optional[Publisher] = Field(default=None) # this is the source info
    chatter: Optional[Chatter] = Field(default=None) # this is the latest chatter info

    # chatter_url: Optional[str] = Field(default=None) # this is the url of the social media post that contains the Bean url
    # chatter_source: Optional[str] = Field(default=None) # this is the domain name of the source
    # forum: Optional[str] = Field(default=None) # this is the
    # likes: Optional[int] = Field(default=0)
    # comments: Optional[int] = Field(default=0)
    # shares: Optional[int] = Field(default=0)
    
    # computed fields related to media
    trend_score: Optional[int] = Field(default=0) # a bean is always similar to itself
    updated: Optional[datetime] = None

    # query result fields
    search_score: Optional[float|int] = None

    def digest(self) -> str:
        text = ""
        if self.created: text += f"U:{self.created.strftime('%Y-%m-%d')};"
        if self.gist: text += self.gist
        # TODO: add entitiies and region down the road
        if self.categories: text += f"C:{'|'.join(self.categories)};"
        if self.sentiments: text += f"S:{'|'.join(self.sentiments)};"
        return text
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        exclude_none = True
        exclude_unset = True
        by_alias=True
        json_encoders={datetime: rfc3339}

# class GeneratedBean(Bean):
#     kind: str = Field(default=OPED)
#     topic: Optional[str] = None
#     intro: Optional[str|list[str]] = None
#     highlights: Optional[list[str]] = None
#     insights: Optional[list[str]] = None
#     predictions: Optional[list[str]] = None

class BeanCore(BaseModel):
    # core fields
    url: str = Field(min_length=1)
    kind: Optional[str] = Field(default=None)
    title: str
    title_length: Optional[int] = Field(default=0, ge=0)
    summary: Optional[str] = Field(default=None)
    summary_length: Optional[int] = Field(default=0, ge=0)
    content: Optional[str] = Field(default=None)
    content_length: Optional[int] = Field(default=0, ge=0)
    restricted_content: Optional[bool] = Field(default=False)
    author: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)
    image_url: Optional[str] = Field(default=None)
    created: Optional[datetime] = Field(default=None)
    collected: Optional[datetime] = Field(default=None)

    # def to_tuple(self) -> tuple:
    #     return (
    #         self.url,
    #         self.kind,
    #         self.title,
    #         self.title_length,
    #         self.summary,
    #         self.summary_length,
    #         self.content,
    #         self.content_length,
    #         self.restricted_content,
    #         self.author,
    #         self.source,
    #         self.image_url,
    #         self.created,
    #         self.collected
    #     )
    
    # def to_list(self) -> list:
    #     return list(self.to_tuple())

    class Config:
        json_encoders={datetime: rfc3339}
        dtype_specs = {            
            'kind': 'string',
            'title': 'string',
            'title_length': 'uint16',
            'summary': 'string',
            'summary_length': 'uint16',
            'content': 'string',
            'content_length': 'uint16',
            'author': 'string',
            'source': 'string',
            'image_url': 'string'
        }

class BeanEmbedding(BaseModel):
    url: str = Field(min_length=1)
    embedding: list = Field(min_length=VECTOR_LEN, max_length=VECTOR_LEN)

    def to_tuple(self) -> tuple:
        return (self.url, self.embedding)

    class Config:
        dtype_specs = {
            'url': 'string',
            'embedding': 'object',
        }

class BeanGist(BaseModel):
    url: str = Field(min_length=1)
    gist: str = Field(min_length=10)
    entities: Optional[list[str]] = None
    regions: Optional[list[str]] = None

    def to_tuple(self) -> tuple:
        return (self.url, self.gist, self.entities, self.regions)

    class Config:
        dtype_specs = {
            'url': 'string',
            'gist': 'string',
            'regions': 'object',  # For list[str]
            'entities': 'object'  # For list[str]
        }

# TODO: DEPRECATE THIS
class ChatterAnalysis(BaseModel):
    url: str
    likes: Optional[int] = 0
    comments: Optional[int] = 0
    shares: Optional[int] = 0
    shared_in: Optional[list[str]] = Field(default=None)
    collected: Optional[datetime] = None
    likes_change: Optional[int] = 0
    comments_change: Optional[int] = 0
    shares_change: Optional[int] = 0
    shared_in_change: Optional[list[str]] = Field(default=None)
    trend_score: Optional[int] = 0

    class Config:
        arbitrary_types_allowed=True
        exclude_none = True
        exclude_unset = True
        exclude_defaults = True
    
class User(BaseModel):
    id: Optional[str] = Field(default=None, alias="_id")  
    email: str = None 
    name: Optional[str] = None
    image_url: Optional[str] = None  
    linked_accounts: Optional[list[str]] = None
    following: Optional[list[str]] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        by_alias=True

class Page(BaseModel):
    id: str = Field(alias="_id")
    title: Optional[str] = None
    description: Optional[str] = None
    created: Optional[datetime] = Field(default_factory=datetime.now)
    owner: Optional[str] = Field(default=SYSTEM)
    public: Optional[bool] = Field(default=False)
    related: Optional[list[str]] = Field(default=None)
    
    query_urls: Optional[list[str]] = Field(default=None, alias="urls")
    query_kinds: Optional[list[str]] = Field(default=None, alias="kinds")
    query_sources: Optional[list[str]] = Field(default=None, alias="sources")   
    query_tags :Optional[list[str]] = Field(default=None, alias="tags")
    query_text: Optional[str] = Field(default=None, alias="text")
    query_embedding: Optional[list[float]] = Field(default=None, alias="embedding")
    query_distance: Optional[float] = Field(default=None, alias="distance")
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        by_alias=True

clean_text = lambda text: text.strip() if text and text.strip() else None
num_words = lambda text: min(len(text.split()) if text else 0, 1<<32)  # SMALLINT max value

def rectify_bean_fields(items: list[Bean|BeanCore]) -> list[Bean|BeanCore]:
    for item in items:
        item.title = clean_text(item.title)
        item.title_length = num_words(item.title)
        item.summary = clean_text(item.summary)
        item.summary_length = num_words(item.summary)
        item.content = clean_text(item.content)
        item.content_length = num_words(item.content)
        item.author = clean_text(item.author)
        item.image_url = clean_text(item.image_url)
        item.created = item.created or now()
        item.collected = item.collected or now()
        if item.author == "[no-author]": item.author = None
        if not item.created.tzinfo: item.created.replace(tzinfo=timezone.utc)
    return items