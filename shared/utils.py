from datetime import datetime, timezone
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

DEFAULT_CHANNELS = [
    {"title": "Artificial Intelligence (AI)", "_id": "artificial-intelligence-ai"},
    {"title": "Automotive Logistics", "_id": "automotive-logistics"},
    {"title": "Aviation & Aerospace", "_id": "aviation-aerospace"},
    {"title": "Business & Finance", "_id": "business-finance"},
    {"title": "Career & Professional Development", "_id": "career-professional-development"},
    {"title": "Cryptocurrency & Blockchain", "_id": "cryptocurrency-blockchain"},
    {"title": "Cybersecurity", "_id": "cybersecurity"},
    {"title": "Entrepreneurship & Startups", "_id": "entrepreneurship-startups"},
    {"title": "Environment & Clean Energy", "_id": "environment-clean-energy"},
    {"title": "Food & Health", "_id": "food-health"},
    {"title": "Gadgets & IoT", "_id": "gadgets-iot"},
    {"title": "Government & Politics", "_id": "government-politics"},
    {"title": "HPC & Datacenters", "_id": "hpc-datacenters"},
    {"title": "Management & Leadership", "_id": "management-leadership"},
    {"title": "Robotics & Manufacturing", "_id": "robotics-manufacturing"},
    {"title": "Science & Mathematics", "_id": "science-mathematics"},
    {"title": "Software Engineering", "_id": "software-engineering"},
    {"title": "Video Games & Virtual Reality", "_id": "video-games-virtual-reality"}
]

DEFAULT_KINDS = [
    {"_id": NEWS, "title": "News"},
    {"_id": POST, "title": "Posts"},
    {"_id": BLOG, "title": "Blogs"}
]


# cache settings
ONE_HOUR = 3600
FOUR_HOURS = 14400
ONE_DAY = 86400
ONE_WEEK = 604800
CACHE_SIZE = 100

def log(logger, function, **kwargs):    
    # transform the values before logging for flat tables
    kwargs["num_returned"] = len(kwargs.get("num_returned", []))
    kwargs = {key: ("|".join(value) if isinstance(value, list) else value) for key, value in kwargs.items() if value}
    logger.info(function, extra=kwargs)

def now():
    return datetime.now()