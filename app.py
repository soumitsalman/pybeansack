import os
import time
from icecream import ic
from dotenv import load_dotenv
from pybeansack import utils

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

oauth = OAuth(Config(env_file=".env"))

# from slack_ui.router import slack_router

# handler = SlackRequestHandler(slack_router)

# @app.post("/slack/events", methods=["POST"])
# @app.post("/slack/commands", methods=["POST"])
# @app.post("/slack/actions", methods=["POST"])
# @app.get("/slack/oauth_redirect")
# @app.get("/slack/install")
# def slack_events(req):
#     return handler.handle(req)

def _temp_user():
    return app.storage.user.get("temp_user")

def _set_temp_user(user):
    app.storage.user["temp_user"] = user

def _clear_temp_user():
    if 'temp_user' in app.storage.user:
        del app.storage.user["temp_user"]

def _session_settings() -> dict:
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = web_ui.pages.create_default_settings()
    return app.storage.user['settings']

@app.get("/reddit/login")
async def reddit_login(request: Request):
    redirect_uri = os.getenv('HOST_URL')+"/reddit/redirect"
    return await oauth.reddit.authorize_redirect(request, redirect_uri)

@app.get("/reddit/redirect")
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
    redirect_uri = os.getenv('HOST_URL')+"/slack/redirect"
    return await oauth.slack.authorize_redirect(request, redirect_uri)

@app.get("/slack/redirect")
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
    existing_user = espressops.get_user(authenticated_user)
    if existing_user:
        web_ui.pages.set_session_settings(_session_settings(), existing_user)
        web_ui.pages.set_logged_in_user(_session_settings(), authenticated_user)
        return RedirectResponse("/") 
    else:
        _set_temp_user(authenticated_user)
        return RedirectResponse("/user-registration")

@app.get('/logout')
def logout():
    web_ui.pages.log_out_user(_session_settings())
    return RedirectResponse('/')

@ui.page('/login-failed')
def login_failed(source: str):
    web_ui.pages.render_login_failed(_session_settings(), f'/{source}/login')

@ui.page('/user-registration')
async def user_registration():
    web_ui.pages.render_user_registration(_session_settings(), _temp_user())
    _clear_temp_user()

@ui.page("/")
async def home():   
    web_ui.pages.render_home(_session_settings())

@ui.page("/search")
async def search(q: str=None, keyword: str=None, kind: str|list[str]=None, days: int=web_ui.defaults.DEFAULT_WINDOW):  
    days = min(days, web_ui.defaults.MAX_WINDOW)
    web_ui.pages.render_search(_session_settings(), q, keyword, kind, days)

@ui.page("/trending")
async def trending(category: str=None, days: int=web_ui.defaults.DEFAULT_WINDOW):  
    days = min(days, web_ui.defaults.MAX_WINDOW) 
    web_ui.pages.render_trending(_session_settings(), category, days)

def initialize_server():
    beanops.initiatize(config.db_connection_str(), None)
    espressops.initialize(config.db_connection_str())
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
