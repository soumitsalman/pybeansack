from functools import cached_property
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

DIGEST_COLUMNS = [K_URL, K_CREATED, K_GIST]
CONTENT_COLUMNS = [K_URL, K_CREATED, K_SOURCE, K_TITLE, K_CONTENT]

class Chatter(BaseModel):
    """Social media chatter or comments that mention an article URL."""
    chatter_url: Optional[str] = Field(default=None, min_length=1, description="The URL of the social media post that contains the article URL.")
    url: str = Field(min_length=1, description="The URL of the article mentioned in the social media post/comment.")
    source: Optional[str] = Field(default=None, description="The id of the source/publisher of the chatter.")
    forum: Optional[str] = Field(default=None, description="The group or forum from which the chatter was collected.")
    collected: Optional[datetime] = Field(default=None, description="The date and time when the chatter was collected.")
    likes: int = Field(default=0, description="The number of likes on the chatter.")
    comments: int = Field(default=0, description="The number of comments on the chatter.")
    subscribers: int = Field(default=0, description="The number of subscribers to the forum or group.")

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
        populate_by_name = True
        arbitrary_types_allowed=False
        exclude_none = True
        exclude_unset = True
        by_alias=True
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
    """The website, publisher or social media from which an article or chatter is sourced."""
    source: str = Field(min_length=1, description="The domain name that matches the source field in Bean.")
    base_url: str = Field(min_length=1, description="The base URL of the publisher.")
    site_name: Optional[str] = Field(default=None, description="The name of the site.")
    description: Optional[str] = Field(default=None, description="A description of the publisher.")
    favicon: Optional[str] = Field(default=None, description="The URL of the publisher's favicon.")
    rss_feed: Optional[str] = Field(default=None, description="The URL of the publisher's RSS feed.")
    collected: Optional[datetime] = Field(default=None, description="The date and time when the publisher information was collected.")

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        exclude_none = True
        exclude_unset = True
        by_alias=True
        json_encoders={datetime: rfc3339}
        dtype_specs = {
            'source': 'string',
            'base_url': 'string',
            'site_name': 'string',
            'description': 'string',
            'favicon': 'string',
            'rss_feed': 'string'
        }

class Bean(BaseModel):    
    """An article such as a news or blog post."""
    url: str = Field(description="The URL of the article.")
    kind: Optional[str] = Field(default=None, description="The kind/type of the article, e.g., news, blog, oped, job, post.")
    source: Optional[str] = Field(default=None, description="The source/publisher id of the article.")
    title: Optional[str] = Field(default=None, description="The title of the article.")
    title_length: Optional[int] = Field(default=None, description="The length of the title in words.")
    summary: Optional[str] = Field(default=None, description="A summary of the article.")
    summary_length: Optional[int] = Field(default=None, description="The length of the summary in words.")
    content: Optional[str] = Field(default=None, description="The full content of the article.")
    content_length: Optional[int] = Field(default=None, description="The length of the content in words.")
    restricted_content: Optional[bool] = Field(default=None, description="Indicates if the content is restricted.")
    image_url: Optional[str] = Field(default=None, description="The URL of the article's image.")
    author: Optional[str] = Field(default=None, description="The author of the article (if available).")
    created: Optional[datetime] = Field(default=None, description="The published date of the article.")
    collected: Optional[datetime] = Field(default=None, description="The date when the article was collected into the system.")

    # llm fields
    embedding: Optional[list[float]] = Field(default=None, description="The embedding vector for the article.")
    gist: Optional[str] = Field(default=None, description="A gist or key points of the article.")
    entities: Optional[list[str]] = Field(default=None, description="Named entities mentioned in the article.")
    regions: Optional[list[str]] = Field(default=None, description="Geographic regions mentioned in the article.")
    categories: Optional[list[str]] = Field(default=None, description="Categories associated with the article.")
    sentiments: Optional[list[str]] = Field(default=None, description="Sentiments expressed in the article.")
    
    @cached_property
    def digest(self) -> str:
        text = ""
        if self.kind: text += f"{self.kind};"
        if self.created: text += f"{self.created.strftime('%b-%d-%Y')};"
        if self.gist: text += self.gist
        # TODO: add entities and region down the road
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
    id: str = Field(description="The unique identifier of the item.")
    title: Optional[str] = Field(None, description="The title of the item.")
    content: Optional[str] = Field(None, description="The content of the item.")
    embedding: Optional[list[float]] = Field(None, description="The embedding vector of the title and content.")
    created: Optional[datetime] = Field(None, description="The creation timestamp.")
    updated: Optional[datetime] = Field(None, description="The last updated timestamp.")

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        exclude_none = True
        exclude_unset = True
        by_alias=True
        json_encoders={datetime: rfc3339}

class Sip(_CupboardItem):
    mug: Optional[str] = Field(None, description="The slug of the parent mug.")
    related: Optional[list[str]] = Field(None, description="The slugs of related past sips.")
    beans: Optional[list[str]] = Field(None, description="The URLs of the beans.")

class Mug(_CupboardItem):
    sips: Optional[list[str]] = Field(None, description="The slugs of the sips or sections.")
    highlights: Optional[list[str]] = Field(None, description="The highlights of the mug.")
    tags: Optional[list[str]] = Field(None, description="The tags associated with the mug.")

class AggregatedBean(Bean, Chatter, Publisher): 
    # adding aggregated bean specific field
    cluster_id: Optional[str] = Field(default=None, description="The ID of the cluster this bean belongs to.")
    cluster_size: Optional[int] = Field(default=None, description="The size of the cluster.")
    related: Optional[list[str]] = Field(default=None, description="Related bean URLs.")
    trend_score: Optional[int] = Field(default=None, description="The trend score of the bean.")

    # modifying publisher fields for rendering
    source: Optional[str] = Field(default=None, description="The domain name that matches the source field in Bean.")
    base_url: Optional[str] = Field(default=None, description="The base URL of the publisher.")

    # modifying chatters fields for rendering
    updated: Optional[datetime] = Field(default=None, description="The last updated date during chatter aggregation.")
    likes: Optional[int] = Field(default=None, description="The number of likes.")
    comments: Optional[int] = Field(default=None, description="The number of comments.")
    shares: Optional[int] = Field(default=None, description="The number of shares.")
    subscribers: Optional[int] = Field(default=None, description="The number of subscribers.")

    # query support fields    
    distance: Optional[float|int] = Field(default=None, description="The distance score for queries.")

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        exclude_none = True
        exclude_unset = True
        by_alias=True
        json_encoders={datetime: rfc3339}       
    
class User(BaseModel):
    id: Optional[str] = Field(default=None, alias="_id", description="The unique identifier of the user.")
    email: str = Field(description="The email address of the user.")
    name: Optional[str] = Field(default=None, description="The name of the user.")
    image_url: Optional[str] = Field(default=None, description="The URL of the user's profile image.")
    linked_accounts: Optional[list[str]] = Field(default=None, description="List of linked account identifiers.")
    following: Optional[list[str]] = Field(default=None, description="List of users or entities the user is following.")
    created: Optional[datetime] = Field(default=None, description="The creation date of the user account.")
    updated: Optional[datetime] = Field(default=None, description="The last updated date of the user account.")

    class Config:
        populate_by_name = True
        arbitrary_types_allowed=False
        by_alias=True

class Page(BaseModel):
    id: str = Field(alias="_id", description="The unique identifier of the page.")
    title: Optional[str] = Field(default=None, description="The title of the page.")
    description: Optional[str] = Field(default=None, description="A description of the page.")
    created: Optional[datetime] = Field(default_factory=datetime.now, description="The creation date of the page.")
    owner: Optional[str] = Field(default=SYSTEM, description="The owner of the page.")
    public: Optional[bool] = Field(default=False, description="Indicates if the page is public.")
    related: Optional[list[str]] = Field(default=None, description="Related page identifiers.")
    
    query_urls: Optional[list[str]] = Field(default=None, alias="urls", description="Query URLs for the page.")
    query_kinds: Optional[list[str]] = Field(default=None, alias="kinds", description="Query kinds for the page.")
    query_sources: Optional[list[str]] = Field(default=None, alias="sources", description="Query sources for the page.")
    query_tags :Optional[list[str]] = Field(default=None, alias="tags", description="Query tags for the page.")
    query_text: Optional[str] = Field(default=None, alias="text", description="Query text for the page.")
    query_embedding: Optional[list[float]] = Field(default=None, alias="embedding", description="Query embedding for the page.")
    query_distance: Optional[float] = Field(default=None, alias="distance", description="Query distance for the page.")
    
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
num_words = lambda text: min(len(text.split()) if text else 0, (1<<15)-1)  # SMALLINT max value

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
        item.site_name = clean_text(item.site_name)
        item.collected = item.collected or now()

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