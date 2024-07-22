import json
from shared import beanops, userops, config
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from icecream import ic
from .renderer import *
from .defaults import *
from shared import prompt_parser

parser = prompt_parser.InteractiveInputParser()

def render_home(settings):
    _render_shell(settings)

    ui.markdown(settings_markdown(settings['search']))
    nuggets = beanops.trending_keyphrases(1, 10)
    with ui.row().classes("gap-0"):
        [render_tag(nugget) for nugget in nuggets]

async def render_trending(settings: dict, category: str, last_ndays: int, topn: int):  
    _render_shell(settings)

    bean_kinds = (datamodels.ARTICLE, datamodels.POST)       
    def render_nugget_as_expandable_item(nugget: Nugget):        
        @ui.refreshable
        def render_beans(load_items: bool):
            if load_items:
                render_beans_as_list(beanops.get_beans_by_nugget(nugget.id, bean_kinds, last_ndays, topn))
            
        bean_count = beanops.count_beans_for_nugget(nugget.id, bean_kinds, last_ndays, topn) 
        with ui.item() as view:
            with ui.column(align_items="start", wrap=True).classes("w-full"):                        
                render_nugget_banner(nugget)  
                with ui.expansion(
                        group="group", 
                        text=nugget.description, 
                        caption=f"{counter_text(bean_count)} items",
                        on_value_change=lambda: render_beans.refresh(beans_panel.value),
                        value=False).classes("w-full") as beans_panel:
                    render_beans(False)                                
        return view

    ui.label(category).classes("text-h5")
    nuggets = await run.io_bound(beanops.highlights, category, last_ndays, topn)     
    if nuggets:
        render_nuggets_as_list(nuggets, render_nugget_as_expandable_item)        
    else:
        ui.label(messages.NOTHING_TRENDING_IN%last_ndays)

async def render_search(settings, query: str, keyword: str, kind, last_ndays: int, topn: int):
    _render_shell(settings)  

    process_prompt = lambda: _trigger_search(settings, prompt_input.value)   
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).on('keydown.enter', process_prompt) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center') as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    ui.label("Examples: "+", ".join(EXAMPLE_OPTIONS)).classes('text-caption self-center')
    
    async def _run_search():
        if keyword:
            return (beanops.count_beans_by_keyword(keyword, topn),
                lambda start: beanops.get_beans_by_keyword(keyword, start, PAGE_LIMIT))
        elif query:
            items = beanops.search(query, tuple(kind) if kind else None, last_ndays, topn)
            return (len(items) if items else 0,
                lambda start: items[start: start+PAGE_LIMIT] if items else None)
        return (None, None)

    banner = query or keyword    
    if banner:
        # means there can be a search result
        ui.label(banner).classes("text-h5")
        count, beans_iter = await _run_search()
        if count:
            render_beans_as_paginated_list(count, beans_iter)
        else:
            ui.label(messages.NOTHING_FOUND)

def _trigger_search(settings, prompt):   
    task, query, ctype, ndays, limit = parser.parse(prompt, settings['search'])
    if task in ["lookfor", "search"]:
        ui.navigate.to(make_url("/search", q=query, kind=ctype, days=ndays, topn=limit))
    elif task == "trending":
        ui.navigate.to(make_url("/trending", category=query, days=ndays, topn=limit))
    else:
        ui.navigate.to(make_url("/search", q=prompt, days=ndays, topn=limit))

def _render_shell(settings):
    # set themes  
    ui.colors(secondary=SECONDARY_COLOR)
    ui.add_css(content=CSS)
    
    def render_topic(topic):
        with ui.item(text=topic, on_click=lambda: ui.navigate.to(make_url("/trending", category=topic, days=settings['search']['last_ndays'], topn=settings['search']['topn']))):
            ui.badge(beanops.count_highlights(topic, last_ndays=settings['search']['last_ndays'], topn=settings['search']['topn'])).props("transparent").style("margin-left: 10px;")

    # header
    with ui.header().classes(replace="row"):
        with ui.avatar(square=True):
            ui.image("images/cafecito.png")
        with ui.button_group().props("unelevated"):
            ui.button(text="Home", icon='home', on_click=lambda: ui.navigate.to("/"))
            ui.button(text="Search", icon="search", on_click=lambda: ui.navigate.to('/search'))
            with ui.dropdown_button(text="Trending", icon='trending_up').props("unelevated"):
                BindableList(render_topic).bind_items_from(settings['search'], 'topics')

        ui.space()
        ui.button(on_click=lambda: settings_drawer.toggle(), icon="settings").props('flat color=white').classes("self-right")

    # settings
    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        _render_settings(settings) 

def _render_settings(settings):   
    with ui.list():
        ui.item_label('Default Settings').classes("text-subtitle1")
        with ui.item():
            with ui.item_section().bind_text_from(settings['search'], "last_ndays", lambda x: f"Last {x} days"):
                ui.slider(min=MIN_WINDOW, max=MAX_WINDOW, step=1).bind_value(settings['search'], "last_ndays")
        with ui.item():
            with ui.item_section().bind_text_from(settings['search'], "topn", lambda x: f"Top {x} results"):
                ui.slider(min=MIN_LIMIT, max=MAX_LIMIT, step=1).bind_value(settings['search'], "topn")
        with ui.item():
            with ui.expansion("Topics of Interest", caption="Select topics your are interesting in"):
                ui.select(options=userops.get_topics(userops.EDITOR, text_only=True), multiple=True).bind_value(settings['search'], 'topics').props("use-chips")
    
    ui.separator()

    with ui.column(align_items="stretch"):
        ui.label('Connections').classes("text-subtitle1")
        ui.switch(text="Slack")
        ui.switch(text="Reddit")
        ui.switch(text="LinkedIn")

def create_default_settings():
    return {
        "search": {
            "last_ndays": DEFAULT_WINDOW,
            "topn": DEFAULT_LIMIT,
            "topics": userops.get_topics(userops.EDITOR, text_only=True)
        },
        "connections": {
            config.REDDIT: None,
            config.SLACK: None
        }            
    }
