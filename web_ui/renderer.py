from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui
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
tag_route = lambda tag: ui.navigate.to(make_url("/search", keyword=tag))
ellipsis_text = lambda text: text if len(text)<=30 else f"{text[:27]}..."

def make_url(target, **kwargs):
    url_val = URL().with_path(target)    
    if kwargs:
        url_val = url_val.with_query(**kwargs)
    return str(url_val)

def set_tag_route(func):
    global tag_route
    tag_route = func

def settings_markdown(settings: dict):
    return "Currently showing news, blogs and social media posts on %s trending in the last **%d** days." % \
        (", ".join([f"**{topic}**" for topic in settings['topics']]), settings['last_ndays'])      

def render_tags(tags: list[str]):
    with ui.row().classes("gap-0") as view:
        [ui.chip(text, on_click=lambda text=text: tag_route(text)).props('outline') for text in tags]
    return view

def render_text_banner(banner: str):
    with ui.label(banner).classes("text-h5") as view:
        ui.separator().style("margin-top: 5px;")
    return view

render_separator = lambda: ui.separator().style("height: 5px; margin: 0px; padding: 0px;") 

def _shortened_markdown(text, emoji = None, url = None):
    text = ellipsis_text(str(text))
    if url:
        text = f"[{text}]({url})"
    if emoji:
        text = emoji+" "+text
    return ui.markdown(text)

def render_bean_body(bean: Bean, show_highlights: bool):    
    ui.label(bean.highlights[0] if bean.highlights else bean.title).classes("text-bold")
    if show_highlights:
        contents = "\n\n".join(["- "+highlight for highlight in bean.highlights]) \
                if bean.highlights else bean.summary
    else:
        contents = bean.summary
    ui.markdown(contents)

def _render_tags_as_hashtag(bean: Bean):
    format_tag = lambda tag: "#"+"".join(item for item in tag.split())
    if bean.tags:
        return [ui.link(ellipsis_text(format_tag(tag)), target=make_url("/search", keyword=tag)).classes('text-caption') for tag in bean.tags[:3]]

def render_bean_banner(bean: Bean, display_media_stats=True):
    with ui.column().classes('text-caption') as view:
        with ui.row(align_items="center"): 
            if bean.created:
                ui.label(f"📅 {date_to_str(bean.created)}") 
            if bean.source:
                ui.markdown(f"🔗 [{bean.source}]({bean.url})")
            if display_media_stats:
                if bean.author:
                    ui.label(f"✍️ {ellipsis_text(bean.author)}")
                if bean.comments:
                    ui.label(f"💬 {bean.comments}")
                if bean.likes:
                    ui.label(f"👍 {bean.likes}")
        with ui.row():
            _render_tags_as_hashtag(bean)
    return view

def render_bean_as_card(bean: Bean, show_highlight: bool=False):
    with ui.card().classes("w-full") as view:
        render_bean_banner(bean)
        render_bean_body(bean, show_highlights=show_highlight)
    return view

def render_beans_as_list(beans: list[Bean], item_render_func=render_bean_as_card):  
    if beans:  
        with ui.list() as view:
            for bean in beans:
                with ui.item().props("dense"):
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
