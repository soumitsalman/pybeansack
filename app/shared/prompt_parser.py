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
    urls: Optional[list[str]] = None
    query: Optional[str] = None
    category: Optional[str] = None
    tag: Optional[str] = None
    last_ndays: Optional[int] = None
    topn: Optional[int] = None
    min_score: Optional[float] = None
    source: Optional[str] = None

class InteractiveInputParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()        
        self.parser.add_argument('task', help='The main task')
        self.parser.add_argument('urls', nargs='*', help='List of URLs to publish')
        self.parser.add_argument('-q', '--query', help='The search query or category')
        self.parser.add_argument('-t', '--tag', help='The type of content to search or to create.')
        self.parser.add_argument('-d', '--ndays', help='The last N days of data to retrieve. N should be between 1 - 30')
        self.parser.add_argument('-n', '--topn', help='The top N items to retrieve. Must be a positive int')
        self.parser.add_argument('-a', '--acc', help="Precision score for search between 0 - 1")
        self.parser.add_argument('-s', '--source', help='Data source to pull from')
        
        self.parser.format_help()
        
    def parse(self, prompt: str) -> ParseResult: 
        try:
            args = self.parser.parse_args(shlex.split(prompt.lower()))
            return ParseResult(
                task=args.task,
                urls=args.urls,       
                query=args.query, 
                tag=args.tag,
                last_ndays=int(args.ndays) if args.ndays else None, 
                topn=int(args.topn) if args.topn else None,
                min_score=min(1, max(0, float(args.acc))) if args.acc else None,
                source=args.source)
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