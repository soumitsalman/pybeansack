from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from datetime import datetime as dt
from urllib.parse import quote
from shared import messages
from icecream import ic
from yarl import URL

F_NUGGETS = "nuggets"
F_NUGGETCOUNT = "nugget_count"
F_SELECTED = "selected"
F_BEANS = "beans"
F_CATEGORIES = "categories"
F_LISTVIEW = "listview"

F_SEARCH_RESULT = "search_result"
F_PROMPT = "prompt"
F_PROCESSING_PROMPT = "processing_prompt"

nugget_markdown = lambda nugget: (f"**{nugget.keyphrase}**"+((": "+nugget.description) if nugget.description else "")) if nugget else None
counter_text = lambda counter: str(counter) if counter < 100 else str(99)+'+'
tag_route = lambda tag: ui.navigate.to(make_url("/search", keyword=tag))

def make_url(target, **kwargs):
    url_val = URL().with_path(target)    
    if kwargs:
        url_val = url_val.with_query(**kwargs)
    return str(url_val)

def set_tag_route(func):
    global tag_route
    tag_route = func

def settings_markdown(settings: dict):
    return "Topics of Interest: %s\n\nPulling top **%d** items from last **%d** days." % \
        (", ".join([f"**{topic}**" for topic in settings['topics']]), settings['topn'], settings['last_ndays'])      

def render_tag(text):
    ui.chip(text, on_click=lambda text=text: tag_route(text)).props('outline square')

def render_banner(banner):
    with ui.label(banner).classes("text-h5") as view:
        ui.separator().style("margin-top: 5px;")
    return view

def render_bean_with_highlights(bean: Bean):
    with ui.card().classes("w-full"):
        render_bean_banner(bean)
        ui.label(bean.title).classes("text-bold")
        ui.markdown("\n\n".join(["- "+highlight for highlight in bean.highlights])) \
            if bean.highlights else ui.markdown(bean.summary)

def render_bean_with_summary(bean: Bean):
    if bean:
        with ui.card().classes("w-full") as card:
            render_bean_banner(bean)
            ui.label(bean.title).classes("text-bold")
            ui.markdown(bean.summary)
        return card

def render_bean_banner(bean: Bean):
    with ui.column() as view:
        with ui.row(align_items="center").classes('text-caption'): 
            if bean.created:
                ui.label(f"📅 {date_to_str(bean.created)}") 
            if bean.source:
                ui.markdown(f"🔗 [{bean.source}]({bean.url})")   
            if bean.author:
                ui.label(f"✍️ {bean.author}")
            if bean.comments:
                ui.label(f"💬 {bean.comments}")
            if bean.likes:
                ui.label(f"👍 {bean.likes}")
        if bean.tags:
            with ui.row().classes("gap-0"):
                [render_tag(word) for word in bean.tags[:4]]
    return view


def render_bean_highlights(bean: Bean):
    if bean.highlights:
        return ui.markdown("\n\n".join("- "+highlight for highlight in bean.highlights))
    else:
        return ui.markdown(bean.summary)
    
    
# def render_nugget_banner(nugget: Nugget):
#     with ui.row(align_items="center").classes('text-caption') as view:
#         ui.label("📅 "+ date_to_str(nugget.updated))
#         ui.label("🏷️ " + nugget.keyphrase)
#     return view

# def render_nugget_as_card(nugget: Nugget):
#     if nugget:
#         with ui.card().classes('no-shadow border-[1px]') as card:
#             render_nugget_banner(nugget)
#             ui.label(text=nugget.description)
#         return card

# def render_nugget_as_item(nugget: Nugget):
#     with ui.item() as view:
#         with ui.column(wrap=True):   
#             render_nugget_banner(nugget)
#             ui.label(text=nugget.description)
#             ui.separator()
#     return view

def render_item(resp: Bean|str):
    if isinstance(resp, str):
        with ui.list():
            ui.markdown(resp)
    elif isinstance(resp, Bean):
        render_bean_with_summary(resp) 
    # elif isinstance(resp, Nugget):
    #     render_nugget_as_item(resp)

def render_beans_as_list(beans: list[Bean], item_render_func=render_bean_with_summary):  
    if beans:  
        with ui.list() as view:
            for bean in beans:
                with ui.item():
                    item_render_func(bean)
        return view

def render_beans_as_paginated_list(count: int, beans_iter: Callable = lambda index: None):
    page_index = {"page_index": 1}
    page_count = min(MAX_PAGES, -(-count//MAX_ITEMS_PER_PAGE))

    @ui.refreshable
    def render_search_items():
        render_beans_as_list(beans_iter((page_index['page_index']-1)*MAX_ITEMS_PER_PAGE))    
    if count > MAX_ITEMS_PER_PAGE:
        ui.pagination(min=1, max=page_count, direction_links=True, value=page_index['page_index'], on_change=render_search_items.refresh).bind_value(page_index, 'page_index')
    render_search_items()
    if count > MAX_ITEMS_PER_PAGE:
        ui.pagination(min=1, max=page_count, direction_links=True, value=page_index).bind_value(page_index, 'page_index')

# def render_nuggets_as_list(nuggets: list[Nugget], item_render_func = render_nugget_as_item):    
#     if nuggets:
#         with ui.list() as view:
#             [item_render_func(nugget) for nugget in nuggets]
#         return view


# def render_beans_as_bindable_list(viewmodel: dict, beans: str = F_BEANS):    
#     return BindableList(render_bean_as_card).bind_items_from(viewmodel, beans)

# def render_nuggets_as_bindable_list(viewmodel: dict, nuggets: str = F_NUGGETS):
#     return BindableList(render_nugget_as_item).bind_items_from(viewmodel, nuggets)

# def render_items_as_bindable_list(viewmodel: dict, items: str = "items"):
#     return BindableList(render_item).bind_items_from(viewmodel, items)