import os
import time
from icecream import ic
from dotenv import load_dotenv
from pybeansack import utils
from pybeansack.embedding import BeansackEmbeddings
import web_ui.renderer

load_dotenv()
logger = utils.create_logger("Espresso")

from authlib.integrations.starlette_client import OAuth
from starlette.config import Config
from starlette.requests import Request
from starlette.responses import RedirectResponse
from nicegui import app, ui
from shared import beanops, config, espressops
import web_ui.pages
import web_ui.defaults
from slack_bolt.adapter.fastapi import SlackRequestHandler

oauth = OAuth()

# from slack_ui.router import slack_router

# handler = SlackRequestHandler(slack_router)

# @app.post("/slack/events", methods=["POST"])
# @app.post("/slack/commands", methods=["POST"])
# @app.post("/slack/actions", methods=["POST"])
# @app.get("/slack/oauth_redirect")
# @app.get("/slack/install")
# def slack_events(req):
#     return handler.handle(req)

def session_settings() -> dict:
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = web_ui.pages.create_default_settings()
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

def set_logged_in_user(authenticated_user):
    app.storage.user['logged_in_user'] = authenticated_user  
    settings = session_settings() 
    if espressops.PREFERENCES in authenticated_user:        
        settings['search']['last_ndays'] = authenticated_user[espressops.PREFERENCES]['last_ndays']
    settings['search']['topics'] = espressops.get_topics(authenticated_user) or settings['search']['topics']

def log_out_user():
    if 'logged_in_user' in app.storage.user:
        del app.storage.user['logged_in_user']

@app.get("/reddit/login")
async def reddit_login(request: Request):
    redirect_uri = os.getenv('HOST_URL')+"/reddit/oauth-redirect"
    return await oauth.reddit.authorize_redirect(request, redirect_uri)

@app.get("/reddit/oauth-redirect")
async def reddit_redirect(request: Request):    
    try:
        token = await oauth.reddit.authorize_access_token(request)
        user = (await oauth.reddit.get('https://oauth.reddit.com/api/v1/me', token=token)).json()
        return _redirect_after_auth(user['name'], user['id'], "reddit", token)
    except Exception as err:
        ic(err)
        return RedirectResponse("/login-failed?source=reddit")

@app.get("/slack/login")
async def slack_login(request: Request):
    redirect_uri = os.getenv('HOST_URL')+"/slack/oauth-redirect"
    return await oauth.slack.authorize_redirect(request, redirect_uri)

@app.get("/slack/oauth-redirect")
async def slack_redirect(request: Request):
    try:
        token = await oauth.slack.authorize_access_token(request)
        user = (await oauth.slack.get('https://slack.com/api/users.identity', token=token)).json()    
        return _redirect_after_auth(user['user']['name'], user['user']['id'], "slack", token)
    except Exception as err:
        ic(err)
        return RedirectResponse("/login-failed?source=slack")

def _redirect_after_auth(name, id, source, token):
    authenticated_user = {
        "name": name,
        "id_in_source": id,
        "source": source,
        **token
    }
    current_user = logged_in_user()
    registered_user = espressops.get_user(authenticated_user)
    # if a user is already logged in then add this as a connection
    if current_user:
        espressops.add_connection(current_user, authenticated_user)
        current_user[espressops.CONNECTIONS][source]=name
        return RedirectResponse(last_page())
    # else no user is logged in but there is an registered user with this cred then log-in that user    
    elif registered_user:
        set_logged_in_user(registered_user)
        return RedirectResponse(last_page()) 
    # or else this is new session log registration
    else:
        set_temp_user(authenticated_user)
        return RedirectResponse("/user-registration")

@app.get('/logout')
def logout():
    log_out_user()
    return RedirectResponse(last_page())

@ui.page('/login-failed')
async def login_failed(source: str):
    web_ui.pages.render_login_failed(f'/{source}/login', last_page())

@ui.page('/user-registration')
def user_registration():
    web_ui.pages.render_user_registration(
        session_settings(), 
        temp_user(),
        lambda user: [set_logged_in_user(user), clear_temp_user(), ui.navigate.to(last_page())],
        lambda: [clear_temp_user(), ui.navigate.to(last_page())])

@ui.page("/")
def home():  
    settings = session_settings()
    settings['last_page'] = "/" 
    web_ui.pages.render_home(settings, logged_in_user())

@ui.page("/search")
def search(q: str=None, keyword: str=None, kind: str|list[str]=None, days: int=web_ui.defaults.DEFAULT_WINDOW):  
    days = min(days, web_ui.defaults.MAX_WINDOW)
    settings = session_settings()
    settings['last_page'] = web_ui.renderer.make_navigation_target("/search", q=q, keyword = keyword, kind = kind, days = days) 
    web_ui.pages.render_search(settings, logged_in_user(), q, keyword, kind, days)

@ui.page("/trending")
def trending(category: str=None, days: int=web_ui.defaults.DEFAULT_WINDOW):  
    days = min(days, web_ui.defaults.MAX_WINDOW) 
    settings = session_settings()
    settings['last_page'] = web_ui.renderer.make_navigation_target("/trending", category=category, days=days) 
    web_ui.pages.render_trending(settings, logged_in_user(), category, days)

def initialize_server():
    embedder = BeansackEmbeddings(config.embedder_path(), config.EMBEDDER_CTX)
    beanops.initiatize(config.db_connection_str(), embedder)
    espressops.initialize(config.db_connection_str(), embedder)
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
        client_kwargs={'scope': 'identity.basic'},
    )

initialize_server()
ui.run(title=config.APP_NAME, favicon="images/cafecito-ico.ico", storage_secret=os.getenv('INTERNAL_AUTH_TOKEN'), host="0.0.0.0", port=8080, show=False, binding_refresh_interval=0.3)
