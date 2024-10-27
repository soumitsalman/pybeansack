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

def log(logger, function, **kwargs):    
    # transform the values before logging for flat tables
    kwargs["num_returned"] = len(kwargs.get("num_returned", []))
    kwargs = {key: ("|".join(value) if isinstance(value, list) else value) for key, value in kwargs.items() if value}
    logger.info(function, extra=kwargs)

