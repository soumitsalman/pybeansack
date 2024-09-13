from contextlib import contextmanager
from pybeansack.datamodels import *
from shared import beanops, espressops
from web_ui.custom_ui import *
from nicegui import ui
from icecream import ic
from yarl import URL
from shared.messages import *

# themes
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Quicksand&display=swap');
@import url('https://fonts.googleapis.com/css2?family=Bree+Serif&display=swap');
    
body {
    font-family: 'Quicksand', serif;
    font-style: normal;
    color: #0D0D0D;        
}

.text-caption { color: gray; }

.bean-header {
    font-weight: bold;
    word-wrap: break-word; 
    overflow-wrap: break-word;
}

.responsive-container {
    width: 100%;
    max-width: 600px; /* Adjust this to control the width on desktops */
    margin: 0 auto;
    padding: 0 auto;
}

.app-name {
    display: block;
    font-size: 1.3rem;
    justify-content: center;
}

.header-container {
    display: flex;
    align-items: center; /* Aligns items vertically */
    justify-content: space-between; /* Distributes space between items */
    width: 100%;
}

@media (max-width: 600px) {
    .responsive-container {
        max-width: 100%;
    }
}

@media (max-width: 680px) {
    .app-name {
      display: none;
    }
}
"""
SECONDARY_COLOR = "#ADD8E6"
IMAGE_DIMENSIONS = "w-32 h-24"

bean_item_class = lambda is_article: "w-full border-[1px]" if is_article else "w-full"
bean_item_style = "border-radius: 5px; margin-bottom: 5px; padding: 0px;"
tag_route = lambda tag: ui.navigate.to(make_navigation_target("/search", keyword=tag))
ellipsis_text = lambda text, cap: text if len(text)<=cap else f"{text[:cap-3]}..."
is_bean_title_too_long = lambda bean: len(bean.title) >= 175

def make_navigation_target(target, **kwargs):
    url_val = URL().with_path(target)    
    if kwargs:
        url_val = url_val.with_query({key:value for key, value in kwargs.items() if value})
    return str(url_val)

def render_settings_as_text(settings: dict):
    return ui.markdown("Currently showing news, blogs and social media posts on %s." % (", ".join([f"**{espressops.category_label(topic)}**" for topic in settings['topics']])))

def render_separator():
    return ui.separator().style("height: 5px;").classes("w-full m-0 p-0 gap-0")

def render_text_banner(banner: str):
    with ui.label(banner).classes("text-h5") as view:
        ui.separator().style("margin-top: 5px;")
    return view

def render_text(msg: str):
    return ui.label(msg).classes("text-h5 self-center text-center")

def render_header():
    ui.colors(secondary=SECONDARY_COLOR)
    ui.add_css(content=CSS)

    with ui.header().classes(replace="row", add="header-container").classes("w-full") as header:        
        with ui.avatar(square=True, color="transparent").tooltip("Espresso by Cafecit.io"):
            ui.image("images/cafecito.png")
    return header

def render_tags(tags: list[str]):
    with ui.row().classes("gap-0") as view:
        [ui.chip(text, on_click=lambda text=text: tag_route(text)).props('outline dense') for text in tags]
    return view

def render_bean_tags_as_hashtag(bean: Bean):
    format_tag = lambda tag: "#"+"".join(item for item in tag.split())
    if bean.tags:
        return [ui.link(ellipsis_text(format_tag(tag), 30), target=make_navigation_target("/search", keyword=tag)).classes('text-caption') for tag in bean.tags[:3]]

def render_bean_tags_as_chips(bean: Bean):
    with ui.row().classes("gap-0") as view:
        [ui.chip(ellipsis_text(text, 30), on_click=lambda text=text: tag_route(text)).props('outline dense') for text in  bean.tags[:MAX_TAGS_PER_BEAN]] if bean.tags else None
    return view
    
def render_bean_title(bean: Bean):
    return ui.label(bean.title).classes("bean-header")

def render_bean_body(bean: Bean):
    if bean.summary:
        return ui.markdown(ellipsis_text(bean.summary, 1500)).style("word-wrap: break-word; overflow-wrap: break-word; text-align: justify;").tooltip("AI generated (duh!)")

def render_bean_stats(bean: Bean, stack: bool): 
    with (ui.column(align_items="stretch").classes(add="gap-0") if stack else ui.row(align_items="baseline")).classes(add="text-caption").style("margin-top: 1px;") as view:  
        ui.label(date_to_str(bean.created or bean.updated))
        with ui.row():
            if bean.comments:
                ui.label(f"💬 {bean.comments}").tooltip(f"{bean.comments} comments across various social media sources")
            if bean.likes:
                ui.label(f"👍 {bean.likes}").tooltip(f"{bean.likes} likes across various social media sources")
    return view

def render_bean_source(bean: Bean):
    with ui.row(wrap=False, align_items="center").classes("gap-0") as view:
        ui.avatar("img:"+beanops.favicon(bean), size="xs", color="transparent")
        ui.link(ellipsis_text(bean.source, 30), bean.url)
    return view

def render_bean_banner(bean: Bean):
    with ui.row(wrap=False, align_items="start").classes("w-full") as view:            
        if bean.image_url: 
            with ui.element():   
                ui.image(bean.image_url).classes(IMAGE_DIMENSIONS)   
                render_bean_stats(bean, stack=True) 
        with ui.element():
            render_bean_title(bean) 
            if not bean.image_url:        
                render_bean_stats(bean, stack=False)    
    return view

def render_bean_actions(user, bean: Bean, show_related_items: Callable = None):
    related_count = beanops.count_related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=MAX_RELATED_ITEMS+1)
    count_label = rounded_number_with_max(related_count, 5)+" related "+("story" if related_count<=1 else "stories")
    
    async def publish():
        msg = LOGIN_FIRST
        if user:
            msg = PUBLISHED if espressops.publish(user, bean.url) else UNKNOWN_ERROR
        ui.notify(msg)

    with ui.row(align_items="center", wrap=False).classes("text-caption w-full"):
        render_bean_source(bean)
        ui.space()
        with ui.button_group().props("unelevated dense flat text-color=gray size=sm"):            
            ui.button(icon="publish", on_click=publish).props("flat text-color=gray size=sm").tooltip("Publish")
            ui.button(icon="share", on_click=lambda: ui.notify(NO_ACTION)).props("flat text-color=gray size=sm").tooltip("Share")  
            if show_related_items and related_count:
                ExpandButton(text=count_label).on_click(lambda e: show_related_items(e.sender.value)).props("flat text-color=gray size=sm no-caps")

def render_whole_bean(user, bean: Bean):
    with ui.element() as view:
        render_bean_banner(bean)
        render_bean_tags_as_chips(bean)
        render_bean_body(bean)
        render_bean_actions(user, bean, None)
    return view

def render_expandable_bean(user, bean: Bean, show_related: bool = True):
    @ui.refreshable
    def render_related_beans(show_items: bool):   
        related_beans, load_beans = ui.state([])
        if show_items and not related_beans:
            load_beans(beanops.related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=MAX_RELATED_ITEMS))     
        render_beans_as_carousel(related_beans, lambda bean: render_whole_bean(user, bean)).set_visibility(show_items)    
    
    CONTENT_STYLE = 'padding: 0px; margin: 0px; word-wrap: break-word; overflow-wrap: break-word;'
    with ui.expansion().props("dense hide-expand-icon") as view:
        with view.add_slot("header"):
            render_bean_banner(bean)

        with ui.element():                        
            render_bean_tags_as_chips(bean)
            render_bean_body(bean)
            render_bean_actions(user, bean, render_related_beans.refresh if show_related else None)
            if show_related:
                render_related_beans(False)
        ui.query('div.q-expansion-item__header').style(add=CONTENT_STYLE).classes(add="w-full")

    return view

def render_beans_as_paginated_list(get_beans: Callable, items_count: int, bean_render_func: Callable):
    # page_index = {"page_index": 1}
    page_count = min(MAX_PAGES, -(-items_count//MAX_ITEMS_PER_PAGE))

    @ui.refreshable
    def render_search_items():
        page, go_to_page = ui.state(0)
        if items_count > MAX_ITEMS_PER_PAGE:
            page_numbers = ui.pagination(min=1, max=page_count, direction_links=True, value=page+1, on_change=lambda: go_to_page(page_numbers.value - 1))
        render_beans_as_list(get_beans(page*MAX_ITEMS_PER_PAGE), True, bean_render_func)    
        if items_count > MAX_ITEMS_PER_PAGE:
            ui.pagination(min=1, max=page_count, direction_links=True, value=page+1).bind_value(page_numbers, 'value')

    render_search_items()

def render_beans_as_list(beans: list[Bean], render_articles: bool, bean_render_func: Callable):
    with ui.list().props(add="dense" if render_articles else "separator").classes("w-full") as view:        
        for bean in beans:
            with ui.item().classes(bean_item_class(render_articles)).style(bean_item_style):
                bean_render_func(bean)
    return view

def render_beans_as_carousel(beans: list[Bean], bean_render_func: Callable):
    with ui.carousel(animated=True, arrows=True).props(f"swipeable control-color=primary").classes("h-full") as view:          
        for bean in beans:
            with ui.carousel_slide(name=bean.url).style('background-color: lightgray; border-radius: 10px;').classes("h-full"):
                bean_render_func(bean)
    return view

def render_skeleton_beans(count = 3):
    with ui.element().classes("w-full") as holder:
        for _ in range(count):
            with ui.card().props("flat bordered").classes("w-full"):
                with ui.row(align_items="start", wrap=False).classes("w-full"):                    
                    ui.skeleton("rect", width="40%", height="75px")
                    with ui.column().classes("w-full"):
                        ui.skeleton("text", width="100%")
                        ui.skeleton("text", width="20%")     
    return holder 

def _render_bean_banner(bean: Bean, display_media_stats=True):
    with ui.column().classes('text-caption') as view:
        with ui.row(align_items="center"): 
            if bean.created:
                ui.label(f"📅 {date_to_str(bean.created)}") 
            if bean.source:
                ui.markdown(f"🔗 [{bean.source}]({bean.url})")
            if display_media_stats:
                if bean.author:
                    ui.label(f"✍️ {ellipsis_text(bean.author, 30)}")
                if bean.comments:
                    ui.label(f"💬 {bean.comments}")
                if bean.likes:
                    ui.label(f"👍 {bean.likes}")
        with ui.row():
            render_bean_tags_as_hashtag(bean)
    return view

@contextmanager
def disable_button(button: ui.button):
    button.disable()
    try:
        yield
    finally:
        button.enable()