import os
from icecream import ic
from dotenv import load_dotenv
from pybeansack import utils

load_dotenv()
logger = utils.create_logger("Espresso")

from authlib.integrations.starlette_client import OAuth
from starlette.config import Config
from starlette.requests import Request
from starlette.responses import RedirectResponse
from pybeansack.embedding import BeansackEmbeddings
from nicegui import app, ui
from shared import beanops, config, espressops
import web_ui.pages
import web_ui.renderer
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_ui.handler import slack_app

oauth = OAuth()
handler = SlackRequestHandler(slack_app)

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
    settings['search']['topics'] = espressops.get_topics(registered_user) or settings['search']['topics']

def log_out_user():
    if 'logged_in_user' in app.storage.user:
        del app.storage.user['logged_in_user']

@app.post("/slack/events")
@app.post("/slack/commands")
@app.post("/slack/actions")
@app.get("/slack/oauth-redirect")
@app.get("/slack/install")
async def receive_slack_app_events(req: Request):
    # ic(req.base_url, req.path_params, req.query_params, req.state, req.url, await req.body())
    res = await handler.handle(req, addition_context_properties={"TEST":"TEST"})
    # ic(str(res.body))
    return res

@app.get("/slack/login")
async def slack_login(request: Request):
    redirect_uri = os.getenv('HOST_URL')+"/slack/web/oauth-redirect"
    return await oauth.slack.authorize_redirect(request, redirect_uri)

@app.get("/slack/web/oauth-redirect")
async def slack_web_redirect(request: Request):
    try:
        token = await oauth.slack.authorize_access_token(request)
        user = (await oauth.slack.get('https://slack.com/api/users.identity', token=token)).json()    
        return _redirect_after_auth(user['user']['name'], user['user']['id'], "slack", token)
    except Exception as err:
        ic(err)
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
        return _redirect_after_auth(user['name'], user['id'], "reddit", token)
    except Exception as err:
        ic(err)
        return RedirectResponse("/login-failed?source=reddit")

def _redirect_after_auth(name, id, source, token):
    authenticated_user = {
        espressops.NAME: name,
        espressops.SOURCE_ID: id,
        espressops.SOURCE: source,
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
def search(q: str=None, keyword: str=None, kind: str|list[str]=None, days: int=config.DEFAULT_WINDOW):  
    days = min(days, config.MAX_WINDOW)
    settings = session_settings()
    settings['last_page'] = web_ui.renderer.make_navigation_target("/search", q=q, keyword = keyword, kind = kind, days = days) 
    web_ui.pages.render_search(settings, logged_in_user(), q, keyword, kind, days)

@ui.page("/trending")
def trending(category: str=None, days: int=config.DEFAULT_WINDOW):  
    days = min(days, config.MAX_WINDOW) 
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
