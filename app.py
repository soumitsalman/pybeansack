import os
curr_dir = os.path.dirname(os.path.abspath(__file__))

# loading environment variables
from dotenv import load_dotenv

env_path = f"{curr_dir}/.env"
logger_path = f"{curr_dir}/app.log"
embedder_path = f"{curr_dir}/models/nomic.gguf"
load_dotenv(env_path)

# initializing logger
from pybeansack import utils


utils.set_logger_path(logger_path)  
logger = utils.create_logger("CDN")

# setting up routes
from slack_bolt.adapter.fastapi import SlackRequestHandler
from nicegui import app, ui
from shared import config, tools
from web_ui import router, defaults
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

def _get_session_settings():
    if 'settings' not in app.storage.user:
        app.storage.user['settings'] = router.create_default_settings()
    return app.storage.user['settings']

@ui.page("/")
def home():   
    router.render_home(_get_session_settings())

@ui.page("/search")
async def search(q: str=None, keyword: str=None, kind: str|list[str]=None, days: int=defaults.DEFAULT_WINDOW, topn: int=defaults.DEFAULT_LIMIT):  
    days = min(days, defaults.MAX_WINDOW)
    topn = min(topn, defaults.MAX_LIMIT)    
    await router.render_search(_get_session_settings(), q, keyword, kind, days, topn)

@ui.page("/trending")
async def trending(category: str=None, days: int=defaults.DEFAULT_WINDOW, topn: int=defaults.DEFAULT_LIMIT):  
    days = min(days, defaults.MAX_WINDOW)
    topn = min(topn, defaults.MAX_LIMIT)    
    await router.render_trending(_get_session_settings(), category, days, topn)

def start_server():
    tools.initialize(config.get_db_connection_str(), embedder_path, config.get_llm_api_key())
    ui.run(title=config.APP_NAME, favicon="images/cafecito-ico.ico", storage_secret=os.getenv('INTERNAL_AUTH_TOKEN'), host="0.0.0.0", port=8080, show=False, binding_refresh_interval=0.3)

if __name__ in {"__main__", "__mp_main__"}:
    start_server()