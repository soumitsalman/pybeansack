import env
from icecream import ic
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
from shared import beanops, espressops, messages
from shared.utils import *
from nicegui import ui, app
from fastapi import HTTPException, Path, Query, Depends
from web_ui import renderer, vanilla
# from authlib.integrations.starlette_client import OAuth

# oauth = OAuth()
def initialize_server():
    embedder = RemoteEmbeddings(env.llm_base_url(), env.llm_api_key(), env.embedder_model(), env.embedder_n_ctx()) \
        if env.llm_base_url() else \
        BeansackEmbeddings(env.embedder_model(), env.embedder_n_ctx())
    beanops.initiatize(env.db_connection_str(), embedder)
    espressops.initialize(env.db_connection_str(), env.sb_connection_str(), embedder)

def session_settings(**kwargs) -> dict:
    app.storage.user.update(kwargs)
    return app.storage.user

def last_page() -> str:
    return session_settings().get('last_page', "/")

def temp_user(**kwargs):
    if kwargs:
        app.storage.user["temp_user"] = kwargs
    return app.storage.user.get("temp_user")

def current_user(**kwargs):
    if kwargs:
        app.storage.user['current_user'] = kwargs
    return app.storage.user.get('current_user')

# def set_temp_user(user):
#     app.storage.user["temp_user"] = user

def delete_temp_user():
    if 'temp_user' in app.storage.user:
        del app.storage.user["temp_user"]

def validate_channel(channel_id: str):
    if not bool(espressops.get_channel(channel_id)):
        raise HTTPException(status_code=404, detail=f"{channel_id} does not exist")
    return channel_id

def validate_doc(doc_id: str):
    if not bool(os.path.exists(f"docs/{doc_id}")):
        raise HTTPException(status_code=404, detail=f"{doc_id} does not exist")
    return doc_id

@ui.page("/")
async def home():  
    log(logger, 'home')
    session_settings(last_page="/")  
    await vanilla.render_home(current_user())

@ui.page("/channel/{channel_id}")
async def channel(channel_id: str = Depends(validate_channel)): 
    log(logger, 'channel', page_id=channel_id)   
    session_settings(last_page=f"/channel/{channel_id}")
    await vanilla.render_channel(current_user(), channel_id)

@ui.page("/docs/{doc_id}")
async def document(doc_id: str = Depends(validate_doc)):
    log(logger, 'docs', page_id=doc_id)
    await vanilla.render_doc(current_user(), doc_id)  

@ui.page("/search")
async def search(
    q: str = None, 
    acc: float = Query(ge=0, le=1, default=DEFAULT_ACCURACY),
    tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
    kind: list[str] | None = Query(max_length=MAX_LIMIT, default=None)):
    log(logger, 'search', q=q, acc=acc, tag=tag, kind=kind)
    settings = session_settings(last_page=renderer.create_navigation_target("/search", q=q, acc=acc, tag=tag, kind=kind))    
    await vanilla.render_search(current_user(), q, acc, tag, kind)
    
initialize_server()
ui.run(
    title=env.app_name(), 
    storage_secret=env.internal_auth_token(),
    dark=True, 
    favicon="./images/favicon.ico", 
    port=8080, 
    show=False,
    uvicorn_reload_includes="*.py,*/web_ui/styles.css"
)
