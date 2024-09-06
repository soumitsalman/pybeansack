############################################
## USER INPUT PARSER FOR STRUCTURED INPUT ##
############################################

from enum import Enum
import argparse
import shlex
from pybeansack.datamodels import *
from icecream import ic

class ContentType(str, Enum):    
    POSTS = "posts"
    COMMENTS = "comments"
    NEWS = "news"
    BLOGS = "blogs"
    HIGHLIGHTS = "highlights"
    NEWSLETTER = "newsletter"

class ParseResult (BaseModel):
    task: Optional[str] = None
    query: Optional[str] = None
    category: Optional[str|list[str]] = None
    keyword: Optional[str] = None
    kind: Optional[str] = None
    last_ndays: Optional[int] = None
    topn: Optional[int] = None
    channel: Optional[str|list[str]] = None

class InteractiveInputParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('task', help='The main task')
        self.parser.add_argument('-q', '--query', help='The search query')
        self.parser.add_argument('-c', '--category', help='The category of the content')
        self.parser.add_argument('-k', '--keyword', help='The keyword to search with')
        self.parser.add_argument('-t', '--type', help='The type of content to search or to create.')    
        self.parser.add_argument('-d', '--ndays', help='The last N days of data to retrieve. N should be between 1 - 30')
        self.parser.add_argument('-n', '--topn', help='The top N items to retrieve. Must be a positive int')
        self.parser.add_argument('-s', '--source', help='Data source to pull from')
        self.parser.format_help()
        
    def parse(self, prompt: str, defaults: dict): 
        try:
            args = self.parser.parse_args(shlex.split(prompt.lower()))
            return ParseResult(
                task=args.task, 
                query=args.query, 
                category=args.category or defaults.get('topics'),
                keyword=args.keyword,
                # kind=_translate_ctype(getattr(ContentType, args.type.upper(), None)) if args.type else None,
                last_ndays=int(args.ndays or defaults.get('last_ndays')), 
                # topn=int(args.topn or 0),
                channel=args.source)
        except:
            return ParseResult(task=None, query=prompt)

def _translate_ctype(ctype: ContentType):
    if ctype == ContentType.POSTS:
        return POST
    elif ctype == ContentType.NEWS:
        return NEWS
    elif ctype in [ContentType.BLOGS, ContentType.NEWSLETTER]:
        return BLOG
    elif ctype == ContentType.COMMENTS:
        return COMMENT  
    else:
        return ctype.value
    
console_parser = InteractiveInputParser()