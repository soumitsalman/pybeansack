####################
## ARTICLE WRITER ##
####################
from retry import retry
from pybeansack.chains import combine_texts
from pybeansack.utils import create_logger
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate

WRITER_TEMPLATE = """You are a {content_type} writer. Your task is to rewrite one section of a {content_type} on a given topic from the drafts provided by the user. 
From the drafts extract ONLY the contents that are strictly relevant to the topic and write the section based on ONLY that. You MUST NOT use your own knowledge for this. 
The section should have a title and body. The section should be less that 400 words. Output MUST be in markdown format.
Rewrite a section of a {content_type} on topic '{topic}' ONLY based on the following drafts:\n{drafts}"""
WRITER_MODEL = "meta-llama/Meta-Llama-3-8B-Instruct"
WRITER_BATCH_SIZE = 6144
DEFAULT_CONTENT_TYPE = "blog"

class ArticleWriter:
    def __init__(self, llm: str):
        prompt = PromptTemplate.from_template(template=WRITER_TEMPLATE)
        self.chain = prompt | llm | StrOutputParser()

    # highlights, coontents and sources should havethe same number of items
    def write_article(self, highlights: list, drafts:list, sources: list, content_type: str = DEFAULT_CONTENT_TYPE):                  
        article = "## Trending Highlights\n"+"\n".join(['- '+item for item in highlights])+"\n\n"
        for i in range(len(drafts)):                                         
            article += (
                self.write_section(highlights[i], drafts[i], content_type) +
                "\n**Sources:** "+ 
                ", ".join({src[0]:f"[{src[0]}]({src[1]})" for src in sources[i]}.values()) + 
                "\n\n")   
        return article

    # highlights, coontents and sources should havethe same number of items
    def stream_article(self, highlights: list, drafts:list, sources: list, content_type: str = DEFAULT_CONTENT_TYPE):                  
        yield "## Trending Highlights\n"+"\n".join(['- '+item for item in highlights])
        for i in range(len(drafts)):                                   
           yield self.write_section(highlights[i], drafts[i], content_type)
           yield "**Sources:** "+ ", ".join({src[0]:f"[{src[0]}]({src[1]})" for src in sources[i]}.values())

    @retry(tries=3, jitter=10, delay=10, logger=create_logger("article writer"))
    def write_section(self, topic: str, drafts: list[str], content_type: str = DEFAULT_CONTENT_TYPE) -> str:        
        while True:         
            # run it once at least   
            texts = combine_texts(drafts, WRITER_BATCH_SIZE, "\n\n\n")
            # these are the new drafts
            drafts = [self.chain.invoke({"content_type": content_type, "topic": topic, "drafts": text}) for text in texts]                           
            if len(drafts) <= 1:
                return drafts[0]
            
article_writer = None
bean_search_func = None

def initiatize(llm, search_func):
    global article_writer, bean_search_func
    article_writer = ArticleWriter(llm)
    bean_search_func = search_func

DEFAULT_CTYPE_TO_WRITE="newsletter"
def write(topic: str, content_type: str, last_ndays: int, stream: bool = False):
    """Writes a newsletter, blogs, social media posts from trending news articles, social media posts blog articles or news highlights on user interest/topic"""
    nuggets_and_beans = bean_search_func(topic, last_ndays, 5)
    if nuggets_and_beans:    
        highlights = [item[0].digest() for item in nuggets_and_beans]
        makecontents = lambda beans: [f"## {bean.title}\n{bean.text}" for bean in beans]
        initial_content = [makecontents(item[1]) for item in nuggets_and_beans]
        makesources = lambda beans: [(bean.source, bean.url) for bean in beans]
        sources = [makesources(item[1]) for item in nuggets_and_beans]
                
        return article_writer.write_article(highlights, initial_content, sources, content_type)  