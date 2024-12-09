from fastapi.responses import FileResponse
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

registered_pages = {}
get_bean_kind = lambda kind_id: next((kind for kind in DEFAULT_KINDS if kind_id == kind[K_ID]), None)

def initialize_server():
    embedder = RemoteEmbeddings(env.llm_base_url(), env.llm_api_key(), env.embedder_model(), env.embedder_n_ctx()) \
        if env.llm_base_url() else \
        BeansackEmbeddings(env.embedder_model(), env.embedder_n_ctx())
    beanops.initiatize(env.db_connection_str(), embedder)
    espressops.initialize(env.db_connection_str(), env.sb_connection_str(), embedder)
    
    # register a list of specialized pages
    global registered_pages
    # TODO: add registered pages here

def session_settings(**kwargs) -> dict:
    if kwargs:
        app.storage.user.update(kwargs)
    return app.storage.user

def last_page() -> str:
    return session_settings().get('last_page', "/")

def temp_user(**kwargs):
    if kwargs:
        app.storage.browser["temp_user"] = kwargs
    return app.storage.browser.get("temp_user")

def registerd_user(**kwargs):
    if kwargs:
        app.storage.user['registered_user'] = kwargs
    return app.storage.user.get('registered_user')

def current_user():
    user = registerd_user()
    return user[K_ID] if user else app.storage.browser["id"]    

def delete_temp_user():
    if 'temp_user' in app.storage.browser:
        del app.storage.browser["temp_user"]

def validate_barista(barista_id: str):
    barista_id = barista_id.lower()
    if not espressops.get_barista(barista_id):
        raise HTTPException(status_code=404, detail=f"{barista_id} does not exist")
    return barista_id

def validate_registered_page(page_id: str):
    page_id = page_id.lower()
    if page_id not in registered_pages:
        raise HTTPException(status_code=404, detail=f"{page_id} does not exist")
    return page_id

def validate_doc(doc_id: str):
    if not bool(os.path.exists(f"docs/{doc_id}")):
        raise HTTPException(status_code=404, detail=f"{doc_id} does not exist")
    return doc_id

def validate_image(image_id: str):
    if not bool(os.path.exists(f"images/{image_id}")):
        raise HTTPException(status_code=404, detail=f"{image_id} does not exist")
    return image_id

@ui.page("/")
async def home():  
    log(logger, 'home', user_id=current_user())
    session_settings(last_page="/")  
    await vanilla.render_trending_snapshot(registerd_user())

@ui.page("/beans")
async def beans(tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None)):
    log(logger, 'beans', user_id=current_user(), tag=tag)
    session_settings(last_page="/beans", tag=tag)
    if tag:
        await vanilla.render_tags_page(registerd_user(), tag)
    else:
        await vanilla.render_trending_snapshot(registerd_user())

@ui.page("/barista/{barista_id}")
async def barista(barista_id: str = Depends(validate_barista, use_cache=True)): 
    log(logger, 'barista', user_id=current_user(), barista_id=barista_id)   
    session_settings(last_page=f"/barista/{barista_id}")
    await vanilla.render_barista_page(registerd_user(), barista_id)

@ui.page("/search")
async def search(
    q: str = None,
    acc: float = Query(ge=0, le=1, default=DEFAULT_ACCURACY)):
    log(logger, 'search', user_id=current_user(), q=q, acc=acc)
    session_settings(last_page=renderer.create_navigation_target("/search", q=q, acc=acc))    
    await vanilla.render_search(registerd_user(), q, acc)

@ui.page("/docs/{doc_id}")
async def document(doc_id: str = Depends(validate_doc)):
    log(logger, 'docs', user_id=current_user(), page_id=doc_id)
    await vanilla.render_doc(registerd_user(), doc_id)  
    
@ui.page("/images/{image_id}")
async def image(image_id: str = Depends(validate_image)):
    return FileResponse(f"./images/{image_id}", media_type="image/png")

# this has to be the last page because it will catch all non-prefixed routes
@ui.page("/{page_id}")
async def location(page_id: str = Depends(validate_registered_page)):
    log(logger, page_id, user_id=current_user())
    session_settings(last_page=f"/{page_id}")
    await registered_pages[page_id]()
    
    
initialize_server()

ui.add_head_html(renderer.GOOGLE_ANALYTICS_SCRIPT, shared=True)
ui.run(
    title=env.app_name(), 
    storage_secret=env.internal_auth_token(),
    dark=True, 
    favicon="./images/favicon.ico", 
    port=8080, 
    show=False,
    uvicorn_reload_includes="*.py,*/web_ui/styles.css"
)
