from pybeansack.datamodels import *
from shared import beanops
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

tag_route = lambda tag: ui.navigate.to(make_navigation_target("/search", keyword=tag))
ellipsis_text = lambda text: text if len(text)<=30 else f"{text[:27]}..."

def make_navigation_target(target, **kwargs):
    url_val = URL().with_path(target)    
    if kwargs:
        url_val = url_val.with_query(**kwargs)
    return str(url_val)

def render_settings_as_text(settings: dict):
    return ui.markdown("Currently showing news, blogs and social media posts on %s trending in the last **%d** days." % \
        (", ".join([f"**{topic}**" for topic in settings['topics']]), settings['last_ndays']))  

def render_separator():
    return ui.separator().style("height: 5px; margin: 0px; padding: 0px;").classes("w-full")

def render_tags(tags: list[str]):
    with ui.row().classes("gap-0") as view:
        [ui.chip(text, on_click=lambda text=text: tag_route(text)).props('outline dense') for text in tags]
    return view

def render_bean_tags_as_hashtag(bean: Bean):
    format_tag = lambda tag: "#"+"".join(item for item in tag.split())
    if bean.tags:
        return [ui.link(ellipsis_text(format_tag(tag)), target=make_navigation_target("/search", keyword=tag)).classes('text-caption') for tag in bean.tags[:3]]

def render_bean_tags_as_chips(bean: Bean):
    if bean.tags:
        with ui.row().classes("gap-0") as view:
            [ui.chip(text, on_click=lambda text=text: tag_route(text)).props('outline dense') for text in  bean.tags[:MAX_TAGS_PER_BEAN]]
        return view
    
def render_text_banner(banner: str):
    with ui.label(banner).classes("text-h5") as view:
        ui.separator().style("margin-top: 5px;")
    return view

def render_expandable_bean(bean: Bean):
    @ui.refreshable
    def render_related_beans(load_items: bool):     
        related_beans = []   
        if load_items:
            related_beans = beanops.related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=MAX_RELATED_ITEMS)
        render_beans_as_carousel(related_beans, render_whole_bean).set_visibility(load_items)            

    related_count = beanops.count_related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=MAX_RELATED_ITEMS+1)    
    with ui.element() as view:
        with ui.item(on_click=lambda: body_panel.set_visibility(not body_panel.visible)).classes("w-full").style("padding: 0px; margin-bottom: 5px;"):            
            if bean.image_url:    
                with ui.item_section().props("side top"):
                    ui.image(bean.image_url).classes("w-32 h-32")                 
            with ui.item_section().props('top'):                
                render_bean_title(bean)                
                render_bean_stats(bean, False) 
        
        with ui.element() as body_panel:                           
            render_bean_tags_as_chips(bean)
            render_bean_body(bean, False)
            with ui.row(align_items="center"):
                ui.markdown(f"*Read more in [{bean.source}]({bean.url})*").classes("text-caption")
                ui.space()
                if related_count:
                    related_expansion=ui.expansion(
                        caption=f"{rounded_number_with_max(related_count, MAX_RELATED_ITEMS)} related item(s)",
                        on_value_change=lambda: render_related_beans.refresh(related_expansion.value)).style("text-align: right")
                    render_related_beans(False)

        body_panel.set_visibility(False)
    return view

def render_whole_bean(bean: Bean):
    with ui.element() as view:
        with ui.item().style("padding: 0px; margin-bottom: 5px;"):
            if bean.image_url:            
                with ui.item_section().props("side top"):
                    ui.image(bean.image_url).classes("w-28 h-28")  
            with ui.item_section().props('top'):                
                render_bean_title(bean)
                render_bean_stats(bean, True) 
        render_bean_tags_as_chips(bean)
        render_bean_body(bean, bean.highlights)
    return view

def render_bean_title(bean: Bean):
    return ui.label(bean.highlights[0] if bean.highlights else bean.title).classes("text-bold")

def render_bean_body(bean: Bean, highlights):
    return ui.markdown("\n".join(f"- {hl}" for hl in bean.highlights[1:]) if highlights else bean.summary)

def render_bean_stats(bean: Bean, render_source: bool): 
    with ui.row(align_items="baseline").classes("text-caption") as view:   
        if bean.created:
            ui.label(date_to_str(bean.created))
        if bean.comments:
            ui.label(f"💬 {bean.comments}")
        if bean.likes:
            ui.label(f"👍 {bean.likes}")
        if render_source:
            ui.markdown(f"🔗 [{bean.source}]({bean.url})")
    return view

def render_beans_as_paginated_list(count: int, beans_iter: Callable = lambda index: None):
    page_index = {"page_index": 1}
    page_count = min(MAX_PAGES, -(-count//MAX_ITEMS_PER_PAGE))

    @ui.refreshable
    def render_search_items():
        render_beans_as_list(beans_iter((page_index['page_index']-1)*MAX_ITEMS_PER_PAGE), True, render_whole_bean)    
    if count > MAX_ITEMS_PER_PAGE:
        ui.pagination(min=1, max=page_count, direction_links=True, value=page_index['page_index'], on_change=render_search_items.refresh).bind_value(page_index, 'page_index')
    render_search_items()
    if count > MAX_ITEMS_PER_PAGE:
        ui.pagination(min=1, max=page_count, direction_links=True, value=page_index).bind_value(page_index, 'page_index')

def render_beans_as_list(beans, render_articles, bean_render_func):
    with ui.list().props(add="dense" if render_articles else "separator").classes("w-full") as view:        
        for bean in beans:
            with ui.item().classes("w-full border-[1px]" if render_articles else "w-full").style(add="border-radius: 5px; margin-bottom: 5px;" if render_articles else "margin-bottom: 5px;"):
                bean_render_func(bean)
    return view

def render_beans_as_carousel(beans: list[Bean], bean_render_func):
    with ui.carousel(animated=True, arrows=True).props(f"swipeable control-color=darkblue").classes("h-full") as view:          
        for bean in beans:
            with ui.carousel_slide(name=bean.url).style('background-color: lightgray; border-radius: 10px;').classes("h-full"):
                bean_render_func(bean)
    return view

def _render_bean_banner(bean: Bean, display_media_stats=True):
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
            render_bean_tags_as_hashtag(bean)
    return view