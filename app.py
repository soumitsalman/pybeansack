import os
curr_dir = os.path.dirname(os.path.abspath(__file__))

# loading environment variables
from dotenv import load_dotenv

env_path = f"{curr_dir}/.env"
load_dotenv(env_path)

# initializing logger
from pybeansack import utils

logger_path = f"{curr_dir}/app.log"
utils.set_logger_path(logger_path)  
logger = utils.create_logger("CDN")

# setting up routes
from slack_bolt.adapter.fastapi import SlackRequestHandler
from nicegui import app, ui
from shared import config, tools
from web_ui import router
from icecream import ic
# from slack_ui.router import slack_router

# handler = SlackRequestHandler(slack_router)

# @app.post("/slack/events", methods=["POST"])
# @app.post("/slack/commands", methods=["POST"])
# @app.post("/slack/actions", methods=["POST"])
# @app.get("/slack/oauth_redirect")
# @app.get("/slack/install")
# def slack_events(req):
#     return handler.handle(req)

@ui.page("/")
def home():    
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = router.create_default_settings()
    router.load_page("Home", {'settings': app.storage.user['settings']})

@ui.page("/search")
def search():    
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = router.create_default_settings()
    router.load_page("Search", {'settings': app.storage.user['settings']})

@ui.page("/trending")
def trending():    
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = router.create_default_settings()
    router.load_page("Trending", {'settings': app.storage.user['settings']})

@ui.page("/search/nuggets/{keyword}")
def nuggets(keyword):    
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = router.create_default_settings()
    router.load_page("SearchNuggets", {'settings': app.storage.user['settings']}, keyword)

@ui.page("/search/beans/{keyword}")
def beans(keyword):    
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = router.create_default_settings()
    router.load_page("SearchBeans", {'settings': app.storage.user['settings']}, keyword)

def start_server():
    tools.initialize(config.get_db_connection_str(), config.get_embedder_model_path(), config.get_llm_api_key())
    ui.run(title=config.APP_NAME, favicon="images/cafecito-ico.ico", storage_secret=os.getenv('INTERNAL_AUTH_TOKEN'), host="0.0.0.0", port=8080, show=False)

if __name__ in {"__main__", "__mp_main__"}:
    start_server()