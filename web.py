import logging
import os
import time
from fastapi import Query
from authlib.integrations.starlette_client import OAuth
from starlette.requests import Request
from starlette.responses import RedirectResponse
from nicegui import app, ui
from fastapi.responses import FileResponse, Response
from icecream import ic
import env

logging.basicConfig(
    filename=f'espresso-web-{time.strftime("%Y-%m-%d", time.localtime())}.log',
    level=logging.WARNING,
    format='%(asctime)s|%(name)s|%(levelname)s|%(user_id)s|%(message)s|%(page_id)s|%(q)s|%(acc)s|%(tag)s|%(kind)s|%(ndays)s', 
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("WEB")
logger.setLevel(logging.INFO)
oauth = OAuth()

from pybeansack.embedding import *
from shared import beanops, config, espressops, messages
import web_ui.pages
import web_ui.renderer

def log(function, **kwargs):
    user = logged_in_user()
    extra = {"user_id": user[espressops.ID] if user else None, "page_id": None, "q": None, "acc": None, "url": None, "tag": None, "kind": None, "ndays": None}
    extra.update(kwargs)
    logger.info(function, extra=extra)

def session_settings() -> dict:
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = config.default_user_settings()
    return app.storage.user['settings']

def last_page() -> str:
    return session_settings().get('last_page', "/")

def temp_user():
    return app.storage.user.get("temp_user")

def set_temp_user(user):
    app.storage.user["temp_user"] = user

def clear_temp_user():
    if 'temp_user' in app.storage.user:
        del app.storage.user["temp_user"]

def logged_in_user():
    return app.storage.user.get('logged_in_user')

def set_logged_in_user(registered_user):
    app.storage.user['logged_in_user'] = registered_user  
    settings = session_settings() 
    if espressops.PREFERENCES in registered_user:        
        settings['search']['last_ndays'] = registered_user[espressops.PREFERENCES]['last_ndays']
    settings['search']['topics'] = espressops.get_user_category_ids(registered_user) or settings['search']['topics']

def log_out_user():
    if 'logged_in_user' in app.storage.user:
        del app.storage.user['logged_in_user']

@app.get("/web/slack/login")
async def slack_login(request: Request):
    redirect_uri = os.getenv('HOST_URL')+"/web/slack/oauth-redirect"
    return await oauth.slack.authorize_redirect(request, redirect_uri)

@app.get("/web/slack/oauth-redirect")
async def slack_web_redirect(request: Request):
    try:
        token = await oauth.slack.authorize_access_token(request)
        user = (await oauth.slack.get('https://slack.com/api/users.identity', token=token)).json()    
        return _redirect_after_auth(user['user']['name'], user['user']['id'], user['user'].get('image_72'), config.SLACK, token)
    except Exception as err:
        logging.warning(err)
        return RedirectResponse("/login-failed?source=slack")

@app.get("/reddit/login")
async def reddit_login(request: Request):
    redirect_uri = os.getenv('HOST_URL')+"/reddit/oauth-redirect"
    return await oauth.reddit.authorize_redirect(request, redirect_uri)

@app.get("/reddit/oauth-redirect")
async def reddit_redirect(request: Request):    
    try:
        token = await oauth.reddit.authorize_access_token(request)
        user = (await oauth.reddit.get('https://oauth.reddit.com/api/v1/me', token=token)).json()
        return _redirect_after_auth(user['name'], user['id'], user.get('icon_img'), config.REDDIT, token)
    except Exception as err:
        logging.warning(err)
        return RedirectResponse("/login-failed?source=reddit")

def _redirect_after_auth(name, id, image_url, source, token):
    authenticated_user = {
        espressops.NAME: name,
        espressops.SOURCE_ID: id,
        espressops.SOURCE: source,
        espressops.IMAGE_URL: image_url,
        **token
    }
    # if a user is already logged in then add this as a connection
    current_user = logged_in_user()
    if current_user:
        espressops.add_connection(current_user, authenticated_user)
        current_user[espressops.CONNECTIONS][source]=name
        log('connection added')
        return RedirectResponse(last_page())
        
    # if no user is logged in but there is an registered user with this cred then log-in that user    
    registered_user = espressops.get_user(authenticated_user)
    if registered_user:
        set_logged_in_user(registered_user)
        log('logged in')
        return RedirectResponse(last_page()) 

    set_temp_user(authenticated_user)
    return RedirectResponse("/user-registration")

@app.get('/logout')
def logout():
    log('logged out')
    log_out_user()
    return RedirectResponse(last_page())

@app.get("/images/{name}")
async def image(name: str):
    path = "./images/"+name
    if os.path.exists(path):
        return FileResponse(path, media_type="image/png")
    return Response(content=messages.RESOURCE_NOT_FOUND, status_code=404)

@ui.page('/login-failed')
async def login_failed(source: str):
    web_ui.pages.render_login_failed(f'/{source}/login', last_page())

@ui.page('/user-registration')
def user_registration():
    log('user_registration', user_id=temp_user()[espressops.ID] if temp_user() else None)
    web_ui.pages.render_user_registration(
        session_settings(), 
        temp_user(),
        lambda user: [set_logged_in_user(user), clear_temp_user(), ui.navigate.to(last_page())],
        lambda: [clear_temp_user(), ui.navigate.to(last_page())])

@ui.page("/")
def home():  
    settings = session_settings()
    settings['last_page'] = "/" 
    log('home')
    web_ui.pages.render_home(settings, logged_in_user())

@ui.page("/search")
def search(
    q: str = None, 
    acc: float = Query(ge=0, le=1, default=config.DEFAULT_ACCURACY),
    tag: list[str] | None = Query(max_length=config.MAX_LIMIT, default=None),
    kind: list[str] | None = Query(max_length=config.MAX_LIMIT, default=None),
    ndays: int | None = Query(ge=config.MIN_WINDOW, le=config.MAX_WINDOW, default=None)):

    settings = session_settings()
    settings['last_page'] = web_ui.renderer.make_navigation_target("/search", q=q, tag=tag, kind=kind, ndays=ndays, acc=acc)
    log('search', q=q, tag=tag, kind=kind, ndays=ndays, acc=acc)
    web_ui.pages.render_search(settings, logged_in_user(), q, acc, tag, kind, ndays)

@ui.page("/page/{category}")
def trending(
    category: str, 
    ndays: int | None = Query(ge=config.MIN_WINDOW, le=config.MAX_WINDOW, default=config.DEFAULT_WINDOW)):    
    if not web_ui.pages.category_exists(category):
        return Response(content=messages.RESOURCE_NOT_FOUND, status_code=404)
    
    settings = session_settings()
    settings['last_page'] = web_ui.renderer.make_navigation_target(f"/page/{category}", ndays=ndays) 
    log('page', page_id=category, ndays=ndays)
    web_ui.pages.render_trending(settings, logged_in_user(), category.lower(), ndays)

@ui.page("/channel/{userid}")
def user_channel(
    userid: str, 
    ndays: int | None = Query(ge=config.MIN_WINDOW, le=config.MAX_WINDOW, default=config.DEFAULT_WINDOW)):
    if not web_ui.pages.channel_exists(userid):
        return Response(content=messages.RESOURCE_NOT_FOUND, status_code=404)
    
    settings = session_settings()
    settings['last_page'] = web_ui.renderer.make_navigation_target(f"/channel/{userid}", ndays=ndays) 
    log('channel', page_id=userid, ndays=ndays)
    web_ui.pages.render_user_channel(settings, logged_in_user(), userid, ndays)

@ui.page("/docs/{doc}")
async def document(doc: str):
    path = f"./documents/{doc}.md"
    if not os.path.exists(path):
        return Response(content=messages.RESOURCE_NOT_FOUND, status_code=404)
    
    log('docs', page_id=doc)
    web_ui.pages.render_document(session_settings(), logged_in_user(), path)        

def initialize_server():
    embedder = RemoteEmbeddings(env.llm_base_url(), env.llm_api_key(), env.embedder_model(), env.embedder_n_ctx()) \
        if env.llm_base_url() else \
        BeansackEmbeddings(env.embedder_model(), env.embedder_n_ctx())
    beanops.initiatize(env.db_connection_str(), embedder)
    espressops.initialize(env.db_connection_str(), env.sb_connection_str(), embedder)

    oauth.register(
        name=config.REDDIT,
        client_id=config.reddit_client_id(),
        client_secret=config.reddit_client_secret(),
        user_agent=config.APP_NAME,
        authorize_url='https://www.reddit.com/api/v1/authorize',
        access_token_url='https://www.reddit.com/api/v1/access_token', 
        api_base_url="https://oauth.reddit.com/",
        client_kwargs={'scope': 'identity mysubreddits'}
    )
    oauth.register(
        name=config.SLACK,
        client_id=config.slack_client_id(),
        client_secret=config.slack_client_secret(),
        user_agent=config.APP_NAME,
        authorize_url='https://slack.com/oauth/authorize',
        access_token_url='https://slack.com/api/oauth.access',
        client_kwargs={'scope': 'identity.basic,identity.avatar'},
    )

initialize_server()
ui.run(title=config.APP_NAME, favicon="images/favicon.jpg", storage_secret=os.getenv('INTERNAL_AUTH_TOKEN'), host="0.0.0.0", port=8080, show=False, binding_refresh_interval=0.3, dark=True)
