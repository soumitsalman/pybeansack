import env
import logging
from azure.monitor.opentelemetry import configure_azure_monitor

configure_azure_monitor(
    connection_string=env.az_insights_connection_string(), 
    logger_name=env.app_name(), 
    instrumentation_options={"fastapi": {"enabled": True}})  
logger: logging.Logger = logging.getLogger(env.app_name())
logger.setLevel(logging.INFO)

from pybeansack.datamodels import *
from pybeansack.embedding import *
from shared import beanops
from shared.utils import *

def initialize_server():
    embedder = RemoteEmbeddings(env.llm_base_url(), env.llm_api_key(), env.embedder_model(), env.embedder_n_ctx()) \
        if env.llm_base_url() else \
        BeansackEmbeddings(env.embedder_model(), env.embedder_n_ctx())
    beanops.initiatize(env.db_connection_str(), embedder)

from fastapi import FastAPI, Query
from icecream import ic

app = FastAPI(title=env.app_name(), version="0.0.1", description="API for Espresso (Alpha)")

@app.get("/beans", response_model=list[Bean]|None)
async def get_beans(
    url: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(max_length=MAX_LIMIT, default=None), 
    source: list[str] = Query(max_length=MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=MIN_LIMIT, le=MAX_LIMIT, default=MAX_LIMIT)):
    """
    Retrieves the bean(s) with the given URL(s).
    """
    res = beanops.get_beans(url, tag, kind, source, ndays, start, limit)  
    log(logger, 'get_beans', url=url, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_returned=res)
    return res

@app.get("/beans/embeddings", response_model=list[Bean]|None)
async def get_embeddings(url: list[str] | None = Query(max_length=MAX_LIMIT, default=None)):
    res = beanops.get_bean_embeddings(url)
    log(logger, 'get_embeddings', url=url, num_returned=res)
    return res

@app.get("/beans/search", response_model=list[Bean]|None)
async def search_beans(
    q: str, 
    acc: float = Query(ge=0, le=1, default=DEFAULT_ACCURACY),
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(max_length=MAX_LIMIT, default=None), 
    source: list[str] = Query(max_length=MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=MIN_LIMIT, le=MAX_LIMIT, default=DEFAULT_LIMIT)):
    """
    Search beans by various parameters.
    q: query string
    acc: accuracy
    tags: list of tags
    kinds: list of kinds
    source: list of sources
    ndays: last n days
    start: start index
    limit: limit
    """
    res = beanops.search_beans(q, acc, tag, kind, source, ndays, start, limit)
    log(logger, 'search_beans', q=q, acc=acc, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_returned=res)
    return res

@app.get("/beans/trending", response_model=list[Bean]|None)
async def trending_beans(
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(default=None), 
    source: list[str] = Query(max_length=MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=MIN_LIMIT, le=MAX_LIMIT, default=MAX_LIMIT)):
    """
    Retuns a set of unique beans, meaning only one bean from each cluster will be included in the result.
    To retrieve all the beans irrespective of cluster, use /beans endpoint.
    To retrieve the beans related to the beans in this result set, use /beans/related endpoint.
    """
    res = beanops.get_trending_beans(tag, kind, source, ndays, start, limit)
    log(logger, 'trending_beans', tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_returned=res)
    return res

@app.get("/beans/related", response_model=list[Bean]|None)
async def get_related_beans(
    url: str, 
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(default=None), 
    source: list[str] = Query(max_length=MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=None), 
    start: int | None = Query(ge=0, default=0), 
    limit: int | None = Query(ge=MIN_LIMIT, le=MAX_LIMIT, default=MAX_LIMIT)):
    """
    Retrieves the related beans to the given bean.
    """    
    res = beanops.get_related(url, tag, kind, source, ndays, start, limit)
    log(logger, 'get_related_beans', url=url, tag=tag, kind=kind, source=source, ndays=ndays, start=start, limit=limit, num_returned=res)
    return res

@app.get("/beans/chatters", response_model=list[Chatter]|None)
async def get_chatters(url: list[str] | None = Query(max_length=MAX_LIMIT, default=None)):
    """
    Retrieves the latest social media stats for the given bean(s).
    """
    res = beanops.get_chatters(url)
    log(logger, 'get_chatters', url=url, num_returned=res)
    return res

@app.get("/beans/sources", response_model=list|None)
async def get_sources():
    """
    Retrieves the list of sources.
    """
    res = beanops.get_sources()
    log(logger, 'get_sources', num_returned=res)
    return res

@app.get("/beans/tags", response_model=list|None)
async def get_tags():
    """
    Retrieves the list of tags.
    """
    res = beanops.get_tags()
    log(logger, 'get_tags', num_returned=res)
    return res


initialize_server()


