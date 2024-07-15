from shared import beanops, messages, prompt_parser
from pybeansack.datamodels import *
from .items_render import *
from web_ui.custom_ui import *
from nicegui import ui, run

EXAMPLE_OPTIONS = ["trending -t posts -q \"cyber security breches\"", "lookfor -q \"GPU vs LPU\"", "settings -d 7 -n 20"]   
PLACEHOLDER = "Tell me lies, sweet little lies"

def _render_prompt_response(resp):
    if isinstance(resp, str):
        ui.markdown(resp)
    elif isinstance(resp, Bean):
        render_bean_as_card(resp) 
    elif isinstance(resp, Nugget):
        render_nugget_as_card(resp)
    
def render(settings: dict):
    parser = prompt_parser.InteractiveInputParser(settings)
    viewmodel = _create_page_viewmodel()

    async def process_prompt():
        if viewmodel[F_PROMPT]:  
            viewmodel[F_PROCESSING_PROMPT] = True
            viewmodel[F_RESPONSE] = None

            task, query, ctype, ndays, topn = parser.parse(viewmodel[F_PROMPT])
            if task == "trending":
                viewmodel[F_RESPONSE] = (await run.cpu_bound(beanops.trending, query, ctype, ndays, topn)) if ctype != "highlights" else (await run.cpu_bound(beanops.highlights, query, ndays, topn))
                viewmodel[F_RESPONSE_BANNER] = f"{len(viewmodel[F_RESPONSE])} results found: {viewmodel[F_PROMPT]}" if viewmodel[F_RESPONSE] else messages.NOTHING_FOUND         
            elif task in ["lookfor", "search"]:
                viewmodel[F_RESPONSE] = await run.cpu_bound(beanops.search, query, ctype, ndays, topn)
                viewmodel[F_RESPONSE_BANNER] = f"{len(viewmodel[F_RESPONSE])} results found: {viewmodel[F_PROMPT]}" if viewmodel[F_RESPONSE] else messages.NOTHING_FOUND
            elif task == "settings":
                settings = parser.update_defaults(query, ctype, ndays, topn)
                viewmodel[F_RESPONSE] = settings_markdown(settings)
                viewmodel[F_RESPONSE_BANNER] = "Updated settings"
            else:
                beans, nuggets = await run.cpu_bound(beanops.search_all, viewmodel['prompt'], ndays, topn)
                viewmodel[F_RESPONSE] = beans + nuggets
                viewmodel[F_RESPONSE_BANNER] = f"{len(viewmodel[F_RESPONSE])} results found: {viewmodel[F_PROMPT]}" if viewmodel[F_RESPONSE] else messages.NOTHING_FOUND
            
            viewmodel[F_PROMPT] = None
            viewmodel[F_PROCESSING_PROMPT] = False

        
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).bind_value(viewmodel, F_PROMPT) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center').on('keydown.enter', process_prompt) as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    ui.label("Examples: "+", ".join(EXAMPLE_OPTIONS)).classes('text-caption self-center')
    ui.label().bind_text_from(viewmodel, F_RESPONSE_BANNER).classes("text-bold")
    BindableList(_render_prompt_response).bind_items_from(viewmodel, F_RESPONSE).bind_visibility_from(viewmodel, F_RESPONSE)
 
def _create_page_viewmodel():
    return {
        F_PROMPT: None,
        F_PROCESSING_PROMPT: False,
        F_RESPONSE_BANNER: None,
        F_RESPONSE: None
    }