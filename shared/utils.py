import humanize
import tldextract
from urllib.parse import urlparse

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

is_valid_url = lambda url: urlparse(url).scheme in ["http", "https"]
favicon = lambda bean: "https://www.google.com/s2/favicons?domain="+tldextract.extract(bean.url).registered_domain
naturalday = lambda date_val: humanize.naturalday(date_val, format="%a, %b %d")
