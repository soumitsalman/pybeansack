import logging
from cachetools import TTLCache, cached
from icecream import ic
from openai import OpenAI, OpenAIError
from retry import retry
from shared import config
from pybeansack import utils

EMBEDDING_MODEL = "thenlper/gte-large"
FAILED_EMBEDDING = [0.0]*1024
llm_client: OpenAI = None

@cached(TTLCache(maxsize=config.CACHE_SIZE, ttl=config.ONE_HOUR))
def embed(text: str):
    try:
        return _embed(text)
    except:
        return FAILED_EMBEDDING

@retry(tries=5, delay=10, max_delay=30, logger=logging.getLogger("EMBEDDER"))
def _embed(text: str):
    global llm_client
    if not llm_client:
        llm_client = OpenAI(api_key=config.llm_api_key(), base_url=config.llm_base_url())
    if text:
        embedding = llm_client.embeddings.create(model=EMBEDDING_MODEL, input=utils.truncate(text, 496), encoding_format="float")
        return embedding.data[0].embedding
    

# # using deepinfra to get support on rate limits
# digest_llm = ChatDeepInfra(
#     api_key=config.get_digestllm_api_key(),
#     model=config.get_digest_model(),
#     temperature=0.1,
#     max_retries=5,
#     max_tokens=384
# )
# digest_prompt = PromptTemplate(
#     template="""You generate concise summary of blogs, news articles and social media posts. 
#     You are given contents from multiple news articles and social media posts. 
#     Each news article and social media post is delimitered by ```.
#     Based on these you generate summary in a satirical and comical tone.            
    
#     Based on today's ({date}) event: "{description}" summarize the following content and determine future implications.
#     ```
#     {content}
#     ```
#     """,
#     input_variables=["date", "keyphase", "description", "content"]
# )

# digest = digest_prompt | digest_llm | StrOutputParser()

# def _prep_for_digest(beans: list[dict]) -> str:
#     # template = lambda bean: f"PUBLISHER: {bean['source']}\nPUBLISH DATE: {dt.fromtimestamp(bean['updated']).strftime('%Y-%m-%d')}\nCONTENT TYPE: {bean['kind']}\nBODY: {bean['text']}"
#     template = lambda bean: bean['text']
#     return "\n```\n".join([template(b) for b in beans])

# def _create_digest(topic, window, limit) -> list:    
#     # reset the DEFAULT LIMIT
#     nuggets = beanops.trending(topic, window, limit)
#     if not nuggets:
#         return None
    
#     summarize = lambda nug, beans: digest.invoke({
#             "date": dt.now().strftime('%Y-%m-%d'),
#             "keyphrase": nug[config.KEYPHRASE],
#             "description": nug[config.DESCRIPTION],
#             "content": _prep_for_digest(beans)
#         })
    
#     nugs_beans = [(nug, beanops.retrieve_beans(urls=nug[beanops.K_URLS])) for nug in nuggets if beanops.K_URLS in nug]
#     return [(nug, summarize(nug, bean), bean) for nug, bean in nugs_beans]

# @tool
# def create_digest(topic: str = None, window: int = config.DEFAULT_WINDOW) -> list:
#     """Creates/writes/prepares a newsletter or blog like report.
#     It takes into context: news articles or social media posts that got published within specified time window and are on the specified topic

#     Args:
#         topic: A text containing the topic/theme/content of the newletter/blog/news article/social media posts. e.g. Cyber security incident, New iphone release, SBF trial
#         window: The time window of when the contents specified by the `kinds` were published. This is the number of days in the past from current days. Default/recent refers to 2 days in the past
#     Returns:
#         The digest report: A block of text in markdown syntax"""
#     return _create_digest(topic, window, config.DEFAULT_LIMIT)

# @tool
# def get_trending_news_and_posts(topics: list[str] = None, kinds: list[str] = None, window: int = None) -> list[dict]:
#     """Finds/queries news articles or social media posts on specified `topic` that are trending within specified time `window`
    
#     Args:
#         topics: A list of strings representing the topics/themes/contents of the newletter/blog/news article/social media posts. e.g. Cyber security incident, New iphone release, SBF trial
#         kinds: The kinds of content to look into such as news article, social media posts, blogs, newsletter
#         window: The time window of when the contents specified by the `kinds` were published. This is the number of days in the past from current days. Default/recent refers to 2 days in the past
#     Returns:
#         An list of dictionary where each dictionary is represents the metadata of a news event, news article or social media post."""
#     ic(topics, kinds, window)
#     return []

# @tool
# def search_news_and_posts(topics: list[str] = None, kinds: list[str] = None) -> list[dict]:
#     """Finds/queries news articles or social media posts on specified `topic`
    
#     Args:
#         topics: A list of strings representing the topics/themes/contents of the newletter/blog/news article/social media posts. e.g. Cyber security incident, New iphone release, SBF trial
#         kinds: The kinds of content to look into such as news article, social media posts, blogs, newsletter
#     Returns:
#         An list of dictionary where each dictionary is represents the metadata of a news event, news article or social media post."""
#     ic(topics, kinds)
#     return []

# interactive = ChatGroq(
#     api_key=config.get_interactive_api_key(),
#     model=config.get_interactive_model(),
#     temperature=0.5,
#     max_retries=3
# )

# interactive.bind_tools([get_trending_news_and_posts, search_news_and_posts, create_digest])



