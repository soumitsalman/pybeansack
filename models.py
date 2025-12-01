from functools import cached_property
from rfc3339 import rfc3339
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional
from datetime import datetime
from .utils import *

BEANS = "beans"
PUBLISHERS = "publishers"
CHATTERS = "chatters"
MUGS = "mugs"
SIPS = "sips"

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
K_CLUSTER_SIZE = "cluster_size"
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
K_SITE_NAME = "site_name"

SYSTEM = "__SYSTEM__"

DIGEST_COLUMNS = [K_URL, K_CREATED, K_GIST, K_CATEGORIES, K_SENTIMENTS]
CONTENT_COLUMNS = [K_URL, K_CREATED, K_SOURCE, K_TITLE, K_CONTENT]

class Chatter(BaseModel):
    chatter_url: Optional[str] = Field(default=None, min_length=1) # this is the url of the social media post that contains the Bean url
    url: str = Field(min_length=1) # this the url from Bean
    source: Optional[str] = Field(default=None) # this is the domain name of the source
    forum: Optional[str] = Field(default=None) # this is the group/forum the chatter was collected from
    collected: Optional[datetime] = Field(default=None)
    likes: int = Field(default=0)
    comments: int = Field(default=0)
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
            'subscribers': 'uint32'
        }

class Publisher(BaseModel):
    source: str = Field(min_length=1) # this is domain name that gets matched with the source field in Bean
    base_url: str = Field(min_length=1)
    site_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    favicon: Optional[str] = Field(default=None)
    rss_feed: Optional[str] = Field(default=None)
    collected: Optional[datetime] = Field(default=None)

    class Config:
        dtype_specs = {
            'source': 'string',
            'base_url': 'string',
            'site_name': 'string',
            'description': 'string',
            'favicon': 'string',
            'rss_feed': 'string'
        }

class Bean(BaseModel):    
    url: str    
    kind: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)
    title: Optional[str] = Field(default=None)
    title_length: Optional[int] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    summary_length: Optional[int] = Field(default=None)
    content: Optional[str] = Field(default=None)
    content_length: Optional[int] = Field(default=None)
    restricted_content: Optional[bool] = Field(default=None)
    image_url: Optional[str] = Field(default=None)
    author: Optional[str] = Field(default=None)
    created: Optional[datetime] = Field(default=None)
    collected: Optional[datetime] = Field(default=None)

    # llm fields
    embedding: Optional[list[float]] = Field(default=None)
    gist: Optional[str] = Field(default=None)
    entities: Optional[list[str]] = Field(default=None)
    regions: Optional[list[str]] = Field(default=None)
    categories: Optional[list[str]] = Field(default=None)
    sentiments: Optional[list[str]] = Field(default=None)
    
    @cached_property
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
            'image_url': 'string',
            'embedding': 'object',
            'gist': 'string',
            'regions': 'object', 
            'entities': 'object'  
        }

class _CupboardItem(BaseModel):
    id: str = Field(...)
    title: Optional[str] = Field(None, description="This is the title")
    content: Optional[str] = Field(None, description="This is the content")
    embedding: Optional[list[float]] = Field(None, description="This is the embedding vector of title+content")
    created: Optional[datetime] = Field(None, description="This is the created timestamp")
    updated: Optional[datetime] = Field(None, description="This is the updated timestamp")

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        exclude_none = True
        exclude_unset = True
        by_alias=True
        json_encoders={datetime: rfc3339}

class Sip(_CupboardItem):
    mug: Optional[str] = Field(None, description="This is the slug to the parent mug")
    related: Optional[list[str]] = Field(None, description="These are the slugs to related past sips")
    beans: Optional[list[str]] = Field(None, description="These are the urls to the beans")

class Mug(_CupboardItem):
    sips: Optional[list[str]] = Field(None, description="These are the slugs to the sips/sections")
    highlights: Optional[list[str]] = Field(None, description="These are the highlights")   
    tags: Optional[list[str]] = Field(None, description="These are the tags")

class AggregatedBean(Bean, Chatter, Publisher): 
    # adding aggregated bean specific field
    cluster_id: Optional[str] = Field(default=None)
    cluster_size: Optional[int] = Field(default=None)
    related: Optional[list[str]] = Field(default=None)
    trend_score: Optional[int] = Field(default=None) 

    # modifying publisher fields for rendering
    source: Optional[str] = Field(default=None) # this is domain name that gets matched with the source field in Bean
    base_url: Optional[str] = Field(default=None)

    # modifying chatters fields for rendering
    updated: Optional[datetime] = Field(default=None) # only applies during chatter aggregation
    likes: Optional[int] = Field(default=None)
    comments: Optional[int] = Field(default=None)
    shares: Optional[int] = Field(default=None)
    subscribers: Optional[int] = Field(default=None)

    # query support fields    
    distance: Optional[float|int] = None

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        exclude_none = True
        exclude_unset = True
        by_alias=True
        json_encoders={datetime: rfc3339}       
    
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

distinct = lambda items, key: list({getattr(item, key): item for item in items}.values())  # deduplicate by url
non_null_fields = lambda items: list(set().union(*[[k for k, v in item.items() if v] for item in items]))

bean_filter = lambda x: bool(x.title and x.collected and x.created and x.source and x.kind)
chatter_filter = lambda x: bool(x.chatter_url and x.url and (x.likes or x.comments or x.subscribers))
publisher_filter = lambda x: bool(x.source and x.base_url)

clean_text = lambda text: text.strip() if text and text.strip() else None
num_words = lambda text: min(len(text.split()) if text else 0, 1<<15)  # SMALLINT max value

_EXCLUDE_AUTHORS = ["[no-author]", "noreply", "hidden", "admin", "isbpostadmin"]
def prepare_beans_for_store(items: list[Bean]) -> list[Bean]:
    if not items: return items

    for item in items:
        item.url = clean_text(item.url)
        item.kind = clean_text(item.kind)
        item.source = clean_text(item.source)
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
        if item.author and any(ex for ex in _EXCLUDE_AUTHORS if ex in item.author): item.author = None
        if not item.created.tzinfo: item.created.replace(tzinfo=timezone.utc)
    
    items = distinct(items, K_URL)
    return list(filter(bean_filter, items))

def prepare_publishers_for_store(items: list[Publisher]) -> list[Publisher]:
    if not items: return items

    for item in items:        
        item.source = clean_text(item.source)
        item.base_url = clean_text(item.base_url)
        item.favicon = clean_text(item.favicon)
        item.rss_feed = clean_text(item.rss_feed)
        item.description = clean_text(item.description)

    items = distinct(items, K_SOURCE)
    return list(filter(publisher_filter, items))

def prepare_chatters_for_store(items: list[Chatter]) -> list[Chatter]:
    if not items: return items

    for item in items:        
        item.chatter_url = clean_text(item.chatter_url)
        item.url = clean_text(item.url)
        item.forum = clean_text(item.forum)
        item.source = clean_text(item.source)
        
    return list(filter(chatter_filter, items))