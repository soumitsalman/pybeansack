from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from datetime import datetime as dt
from urllib.parse import quote
from shared import messages
from icecream import ic

F_NUGGETS = "nuggets"
F_NUGGETCOUNT = "nugget_count"
F_SELECTED = "selected"
F_BEANS = "beans"
F_CATEGORIES = "categories"
F_LISTVIEW = "listview"

F_SEARCH_RESULT = "search_result"
F_PROMPT = "prompt"
F_PROCESSING_PROMPT = "processing_prompt"

bean_navigate_path = lambda keyword: "/search/beans/"+quote(keyword)
nugget_navigate_path = lambda keyword: "/search/nuggets/"+quote(keyword)

nugget_markdown = lambda nugget: (f"**{nugget.keyphrase}**"+((": "+nugget.description) if nugget.description else "")) if nugget else None
counter_text = lambda counter: str(counter) if counter < 100 else str(99)+'+'
tag_route = lambda tag: ui.notify(messages.NO_ACTION)

def settings_markdown(settings: dict):
    return "Topics of Interest: %s\n\nPulling top **%d** %s from last **%d** days." % \
        (", ".join([f"**{topic}**" for topic in settings['topics']]), settings['topn'], ", ".join([f"**{ctype}**" for ctype in settings['content_types']]), settings['last_ndays'])      

def render_tag(text):
    ui.chip(text, on_click=lambda text=text: tag_route(text)).props('outline square')

def render_bean_as_card(bean: Bean):
    if bean:
        with ui.card() as card:
            with ui.row(align_items="center").classes('text-caption'): 
                if bean.created:
                    ui.label(f"📅 {date_to_str(bean.created)}") 
                if bean.source:
                    ui.markdown(f"🔗 [{bean.source}]({bean.url})")   
                if bean.author:
                    ui.label(f"✍️ {bean.author}")
                if bean.noise and bean.noise.comments:
                    ui.label(f"💬 {bean.noise.comments}")
                if bean.noise and bean.noise.likes:
                    ui.label(f"👍 {bean.noise.likes}")
            if bean.tags:
                with ui.row().classes("gap-0"):
                    [render_tag(word) for word in bean.tags[:3]]
            ui.label(bean.title).classes("text-bold")
            ui.markdown(bean.summary)
        return card
    
def render_nugget_banner(nugget: Nugget):
    with ui.row(align_items="center").classes('text-caption') as view:
        ui.label("📅 "+ date_to_str(nugget.updated))
        render_tag(nugget.keyphrase)
    return view

def render_nugget_as_card(nugget: Nugget):
    if nugget:
        with ui.card().classes('no-shadow border-[1px]') as card:
            render_nugget_banner(nugget)
            ui.label(text=nugget.description)
        return card

def render_nugget_as_item(nugget: Nugget):
    with ui.item() as view:
        with ui.column(wrap=True):   
            render_nugget_banner(nugget)
            ui.label(text=nugget.description)
            ui.separator()
    return view

def render_item(resp: Bean|Nugget|str):
    if isinstance(resp, str):
        with ui.list():
            ui.markdown(resp)
    elif isinstance(resp, Bean):
        render_bean_as_card(resp) 
    elif isinstance(resp, Nugget):
        render_nugget_as_item(resp)
    
def render_beans_as_bindable_list(viewmodel: dict, beans: str = F_BEANS):    
    return BindableList(render_bean_as_card).bind_items_from(viewmodel, beans)

def render_nuggets_as_bindable_list(viewmodel: dict, nuggets: str = F_NUGGETS):
    return BindableList(render_nugget_as_item).bind_items_from(viewmodel, nuggets)

def render_items_as_bindable_list(viewmodel: dict, items: str = "items"):
    return BindableList(render_item).bind_items_from(viewmodel, items)

def render_beans_as_list(beans: list[Bean]):    
    with ui.list() as view:
        for bean in beans:
            with ui.item():
                render_bean_as_card(bean)
    return view

def render_nuggets_as_list(nuggets: list[Nugget]):    
    with ui.list() as view:
        [render_nugget_as_item(nugget) for nugget in nuggets]
    return view
