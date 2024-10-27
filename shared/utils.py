import logging
import os
from pybeansack.datamodels import *

SLACK = "slack"
REDDIT = "reddit"

# search settings
DEFAULT_ACCURACY = 0.75
DEFAULT_WINDOW = 7
MIN_WINDOW = 1
MAX_WINDOW = 30
DEFAULT_LIMIT = 10
MIN_LIMIT = 1
MAX_LIMIT = 100
MAX_ITEMS_PER_PAGE = 5
MAX_PAGES = 10
MAX_TAGS_PER_BEAN = 5
MAX_RELATED_ITEMS = 5

# cache settings
ONE_HOUR = 3600
FOUR_HOURS = 14400
ONE_DAY = 86400
ONE_WEEK = 604800
CACHE_SIZE = 100

# decommissioning this for now
# UNCATEGORIZED = {K_ID:"uncategoried", K_TEXT: "Yo Momma"}
# category settings
DEFAULT_CATEGORIES = [
    "artificial-intelligence-ai", 
    "automotive-logistics",
    "aviation-aerospace", 
    "business-finance", 
    "career-professional-development", 
    "cryptocurrency-blockchain", 
    "cybersecurity", 
    "entrepreneurship-startups", 
    "environment-clean-energy", 
    "food-health", 
    "gadgets-iot", 
    "government-politics", 
    "hpc-datacenters",
    "management-leadership", 
    "robotics-manufacturing", 
    "science-mathematics", 
    "software-engineering", 
    "video-games-virtual-reality"
]

def default_user_settings():
    return {
        "search": {
            "last_ndays": DEFAULT_WINDOW,            
            "topics": DEFAULT_CATEGORIES
        }          
    }

# # deployment settings
# EMBEDDER_CTX = 4096
# def embedder_path():
#     return os.getenv("MODEL_PATH")

# def slack_bot_token() -> str:
#     return os.getenv("SLACKER_BOT_TOKEN")
# def slack_app_token() -> str:
#     return os.getenv("SLACKER_APP_TOKEN")
# def slack_signing_secret() -> str:
#     return os.getenv("SLACKER_SIGNING_SECRET")
# def slack_client_id():
#     return os.getenv("SLACKER_CLIENT_ID")
# def slack_client_secret():
#     return os.getenv("SLACKER_CLIENT_SECRET")

# def reddit_client_id():
#     return os.getenv("REDDITOR_APP_ID")
# def reddit_client_secret():
#     return os.getenv("REDDITOR_APP_SECRET")

# def db_connection_str():
#     return os.getenv("DB_CONNECTION_STRING")
# def sb_connection_str():
#     return os.getenv("SB_CONNECTION_STRING")
# def llm_api_key():
#     return os.getenv("LLM_API_KEY")
# def llm_base_url():
#     return os.getenv("LLM_BASE_URL")

# def host_url():
#     return os.getenv("HOST_URL")

def log(logger, function, **kwargs):    
    # transform the values before logging for flat tables
    kwargs["num_returned"] = len(kwargs.get("num_returned", []))
    kwargs = {key: ("|".join(value) if isinstance(value, list) else value) for key, value in kwargs.items() if value}
    logger.info(function, extra=kwargs)

