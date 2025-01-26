import humanize
import tldextract
from urllib.parse import urlparse
import logging
from app.shared.env import *
from icecream import ic

# cache settings
ONE_HOUR = 3600
FOUR_HOURS = 14400
ONE_DAY = 86400
ONE_WEEK = 604800
CACHE_SIZE = 100

def log(function, **kwargs):    
    # transform the values before logging for flat tables
    kwargs = {key: (str(value) if isinstance(value, list) else value) for key, value in kwargs.items() if value}
    logging.getLogger(APP_NAME).info(function, extra=kwargs)

is_valid_url = lambda url: urlparse(url).scheme in ["http", "https"]
favicon = lambda bean: "https://www.google.com/s2/favicons?domain="+urlparse(bean.url).netloc
naturalday = lambda date_val: humanize.naturalday(date_val, format="%a, %b %d")
user_id = lambda user: user.email if user else None
