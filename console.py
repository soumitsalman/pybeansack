from icecream import ic
from dotenv import load_dotenv
load_dotenv()

from pybeansack.embedding import BeansackEmbeddings
from shared import beanops, espressops, config, messages, prompt_parser
from datetime import datetime as dt

APP_NAME = "Espresso:"

def render_beans(beans):
    if not beans:
        print(APP_NAME, messages.BEANS_NOT_FOUND)  
        return    
    print(APP_NAME, len(beans), "Beans")
    [print(bean.title) for bean in beans]
        
def run_console(): 
    settings = config.default_user_settings()  
    beans = None 
    while True:        
        result = ic(prompt_parser.console_parser.parse(input("You: "), settings['search']))
        if not result.task:
            beans = beanops.search(query=result.query, tags=None, kinds=None, last_ndays=None, min_score=result.min_score, start_index=0, topn=config.MAX_ITEMS_PER_PAGE)   
            
        if result.task in ["lookfor", "search"]: 
            beans = beanops.search(query=result.query, tags=result.tags, kinds=result.kind, last_ndays=result.last_ndays, min_score=result.min_score, start_index=0, topn=config.MAX_ITEMS_PER_PAGE)       
            
        if result.task in ["trending"]: 
            beans = beanops.trending(urls=None, categories=result.query, kinds=result.kind, last_ndays=result.last_ndays, start_index=0, topn=config.MAX_ITEMS_PER_PAGE)     

        if result.task in ["publish"]:
            ic(result.urls)   
            
        if result.task == "exit":
            print("Exiting...")
            break
        

        render_beans(beans)    

# embedder = BeansackEmbeddings(config.embedder_path(), config.EMBEDDER_CTX)
beanops.initiatize(config.db_connection_str())
espressops.initialize(config.db_connection_str(), config.sb_connection_str())
run_console()

#  & 'C:\Program Files\ngrok\ngrok.exe' http --domain=workable-feline-deeply.ngrok-free.app 8080