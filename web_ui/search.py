from shared import beanops, messages, prompt_parser
from pybeansack.datamodels import *
from .render import *
from web_ui.custom_ui import *
from nicegui import ui

EXAMPLE_OPTIONS = ["trending -t posts -q \"cyber security breches\"", "lookfor -q \"GPU vs LPU\"", "settings -d 7 -n 20"]   
PLACEHOLDER = "Tell me lies, sweet little lies"

def render_prompt_response(resp):
    if isinstance(resp, str):
        ui.markdown(resp)
    elif isinstance(resp, Bean):
        render_bean_as_card(resp) 
    elif isinstance(resp, Nugget):
        render_nugget_as_card(resp)
    
def settings_markdown(settings: dict):
    return "Topics of Interest: %s\n\nDefault Content Types: %s\n\nPulling top **%d** items from last **%d** days." % \
        (", ".join([f"**{topic}**" for topic in settings['topics']]), ", ".join([f"**{ctype}**" for ctype in settings['content_types']]), settings['topn'], settings['last_ndays'])      

def render_search_page(viewmodel: dict, settings: dict):
    parser = prompt_parser.InteractiveInputParser(settings)
   
    async def process_prompt():
        if viewmodel[F_PROMPT]:  
            viewmodel[F_PROCESSING_PROMPT] = True
            viewmodel[F_RESPONSE] = None

            task, query, ctype, ndays, topn = parser.parse(viewmodel[F_PROMPT])
            if task == "trending":
                viewmodel[F_RESPONSE] = beanops.trending(query, ctype, ndays, topn) if ctype != "highlights" else beanops.highlights(query, ndays, topn)
                viewmodel[F_RESPONSE_BANNER] = f"{len(viewmodel[F_RESPONSE])} results found: {viewmodel[F_PROMPT]}" if viewmodel[F_RESPONSE] else messages.NOTHING_FOUND         
            elif task in ["lookfor", "search"]:
                viewmodel[F_RESPONSE] = beanops.search(query, ctype, ndays, topn)
                viewmodel[F_RESPONSE_BANNER] = f"{len(viewmodel[F_RESPONSE])} results found: {viewmodel[F_PROMPT]}" if viewmodel[F_RESPONSE] else messages.NOTHING_FOUND
            elif task == "settings":
                settings = parser.update_defaults(query, ctype, ndays, topn)
                viewmodel[F_RESPONSE] = settings_markdown(settings)
                viewmodel[F_RESPONSE_BANNER] = "Updated settings"
            else:
                beans, nuggets = beanops.search_all(viewmodel['prompt'], ndays, topn)
                viewmodel[F_RESPONSE] = beans + nuggets
                viewmodel[F_RESPONSE_BANNER] = f"{len(viewmodel[F_RESPONSE])} results found: {viewmodel[F_PROMPT]}" if viewmodel[F_RESPONSE] else messages.NOTHING_FOUND
            
            viewmodel[F_PROMPT] = None
            viewmodel[F_PROCESSING_PROMPT] = False

        
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).bind_value(viewmodel, F_PROMPT) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center').on('keydown.enter', process_prompt) as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    ui.label("Examples: "+", ".join(EXAMPLE_OPTIONS)).classes('text-caption self-center')
    ui.label().bind_text_from(viewmodel, F_RESPONSE_BANNER).classes("text-bold")
    BindableList(render_prompt_response).bind_items_from(viewmodel, F_RESPONSE).bind_visibility_from(viewmodel, F_RESPONSE)
 