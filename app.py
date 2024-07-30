import os
from pybeansack import utils
from dotenv import load_dotenv

load_dotenv()
QUERY_EMBEDDER = ".models/snowflake-arctic-Q4.GGUF"
logger = utils.create_logger("Espresso")


from pybeansack.embedding import BeansackEmbeddings
from shared import beanops, config, espressops
import web_ui.router
import web_ui.defaults
from slack_bolt.adapter.fastapi import SlackRequestHandler
from nicegui import app, ui
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
        app.storage.user['settings'] = web_ui.router.create_default_settings()
    return app.storage.user['settings']

@ui.page("/")
def home():   
    web_ui.router.render_home(_get_session_settings())

@ui.page("/search")
async def search(q: str=None, keyword: str=None, kind: str|list[str]=None, days: int=web_ui.defaults.DEFAULT_WINDOW, topn: int=web_ui.defaults.DEFAULT_LIMIT):  
    days = min(days, web_ui.defaults.MAX_WINDOW)
    topn = min(topn, web_ui.defaults.MAX_LIMIT)    
    await web_ui.router.render_search(_get_session_settings(), q, keyword, kind, days, topn)

@ui.page("/trending")
async def trending(category: str=None, days: int=web_ui.defaults.DEFAULT_WINDOW, topn: int=web_ui.defaults.DEFAULT_LIMIT):  
    days = min(days, web_ui.defaults.MAX_WINDOW)
    topn = min(topn, web_ui.defaults.MAX_LIMIT)    
    await web_ui.router.render_trending(_get_session_settings(), category, days, topn)

def start_server():
    # tools.initialize(config.get_db_connection_str(), QUERY_EMBEDDER, config.get_llm_api_key())
    # def initialize(db_conn_str, embedder_path, llm_api_key):
    # embedder = BeansackEmbeddings(QUERY_EMBEDDER, 2000)
    
    beanops.initiatize(config.get_db_connection_str(), BeansackEmbeddings(QUERY_EMBEDDER, 2000))
    espressops.initialize(config.get_db_connection_str())
    # writer.initiatize(
    #   ChatGroq(api_key=llm_api_key, model="llama3-8b-8192", temperature=0.1, verbose=False, streaming=False), 
    #   lambda topic, last_ndays, limit: beanops.search_all(topic, last_ndays, limit))

    ui.run(title=config.APP_NAME, favicon="images/cafecito-ico.ico", storage_secret=os.getenv('INTERNAL_AUTH_TOKEN'), host="0.0.0.0", port=8080, show=False, binding_refresh_interval=0.3)

if __name__ in {"__main__", "__mp_main__"}:
    start_server()