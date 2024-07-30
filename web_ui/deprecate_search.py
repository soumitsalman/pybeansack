from shared import beanops, messages, prompt_parser
from pybeansack.datamodels import *
from .renderer import *
from web_ui.custom_ui import *
from nicegui import ui, run

EXAMPLE_OPTIONS = ["trending -t posts -q \"cyber security breches\"", "search -q \"GPU vs LPU\"", "settings -d 7 -n 20"]   
PLACEHOLDER = "Tell me lies, sweet little lies"
   
def render(viewmodel: dict):
    viewmodel = _init_page_viewmodel(viewmodel)
    searchmodel = viewmodel['console']
    parser = prompt_parser.InteractiveInputParser( viewmodel['settings']['search'])

    async def process_prompt():
        if searchmodel[F_PROMPT]:  
            searchmodel[F_PROCESSING_PROMPT] = True

            task, query, ctype, ndays, topn = parser.parse(searchmodel[F_PROMPT])
            
            if task == "trending":
                items = beanops.trending(query, ctype, ndays, topn) if ctype != "highlights" else beanops.highlights(query, ndays, topn)                        
            elif task in ["lookfor", "search"]:
                items = beanops.search(query, ctype, ndays, topn)
            elif task == "settings":
                settings = parser.update_defaults(query, ctype, ndays, topn)
                items = [settings_markdown(settings)]
            else:
                beans, nuggets = beanops.search_all(searchmodel['prompt'], ndays, topn)
                items = beans + nuggets
            
            if task == "settings":
                banner = "Settings updated"
            else:
                banner = searchmodel[F_PROMPT] if items else messages.NOTHING_FOUND

            searchmodel[F_SEARCH_RESULT] = ((lambda start, limit: items[start: start+limit] if items else None), (len(items) if items else 0), banner)
            searchmodel[F_PROMPT] = None
            searchmodel[F_PROCESSING_PROMPT] = False
        
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).bind_value(searchmodel, F_PROMPT) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center').on('keydown.enter', process_prompt) as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    ui.label("Examples: "+", ".join(EXAMPLE_OPTIONS)).classes('text-caption self-center')
    BindablePaginatedList(render_item).bind_contents_from(searchmodel, F_SEARCH_RESULT).classes("w-full")

def load_beans_by_keyword(viewmodel, keyword):
    viewmodel['console'][F_SEARCH_RESULT] = ((lambda start, limit: beanops._run_query(keyword, start, limit)), beanops.count_beans(keyword, MAX_LIMIT), keyword)
    
def load_nuggets_by_keyword(viewmodel, keyword):
    viewmodel['console'][F_SEARCH_RESULT] = ((lambda start, limit: beanops.get_nuggets_by_keyword(keyword, start, limit)), beanops.count_nuggets_by_keyword(keyword, MAX_LIMIT), keyword)
    
def _init_page_viewmodel(viewmodel: dict):
    if not viewmodel.get('console'):
        viewmodel['console'] = {
            F_PROMPT: None,
            F_PROCESSING_PROMPT: False,
            F_SEARCH_RESULT: (None, None, None)
        }
    return viewmodel
