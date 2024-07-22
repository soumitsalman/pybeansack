############################################
## USER INPUT PARSER FOR STRUCTURED INPUT ##
############################################

from enum import Enum
import argparse
import shlex
from pybeansack.datamodels import *

_ALL = [ARTICLE, POST, COMMENT]

class ContentType(str, Enum):    
    POSTS = "posts"
    COMMENTS = "comments"
    NEWS = "news"
    BLOGS = "blogs"
    HIGHLIGHTS = "highlights"
    NEWSLETTER = "newsletter"



class InteractiveInputParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('task', help='The main task')
        self.parser.add_argument('-q', '--query', help='The search query')
        self.parser.add_argument('-k', '--keyword', help='The keyword to search with')
        self.parser.add_argument('-t', '--type', help='The type of content to search or to create.')    
        self.parser.add_argument('-d', '--ndays', help='The last N days of data to retrieve. N should be between 1 - 30')
        self.parser.add_argument('-n', '--topn', help='The top N items to retrieve. Must be a positive int')
        self.parser.add_argument('-s', '--source', help='Data source to pull from')
        self.parser.format_help()
        
    def parse(self, prompt: str, defaults: dict):        
        try:
            args = self.parser.parse_args(shlex.split(prompt.lower()))      
            # parse query/topics            
            query = [item.strip() for item in args.query.split(",")] if args.query else defaults.get('topics', [])
            # parser content_types/kind
            ctypes = [_translate_ctype(getattr(ContentType, item.strip().upper(), None)) for item in args.type.split(",")] if args.type else _ALL
            ndays = int(args.ndays) if args.ndays else defaults.get('last_ndays')
            topn = int(args.topn) if args.topn else defaults.get('topn')
            return (args.task, _tuplify_if_many(query), _tuplify_if_many(ctypes) , ndays, topn)
        except:
            return (None, defaults.get('topics', []), _tuplify_if_many(_ALL), defaults.get('last_ndays'), defaults.get('topn'))
        

def _tuplify_if_many(items):
    if isinstance(items, list):
        return tuple(items) if len(items) > 1 else items[0]
    else:
        return items     

def _translate_ctype(ctype: ContentType):
    if ctype == ContentType.POSTS:
        return POST
    elif ctype in [ContentType.NEWS, ContentType.BLOGS, ContentType.NEWSLETTER]:
        return ARTICLE
    elif ctype == ContentType.COMMENTS:
        return COMMENT  
    else:
        return ctype.value