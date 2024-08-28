from pybeansack.datamodels import *
from shared import beanops
from web_ui.custom_ui import *
from nicegui import ui
from icecream import ic
from yarl import URL

bean_item_class = lambda is_article: "w-full border-[1px]" if is_article else "w-full"
bean_item_style = "border-radius: 5px; margin-bottom: 5px; padding: 0px;"
tag_route = lambda tag: ui.navigate.to(make_navigation_target("/search", keyword=tag))
ellipsis_text = lambda text: text if len(text)<=40 else f"{text[:37]}..."
is_bean_title_too_long = lambda bean: len(bean.title) >= 175

def make_navigation_target(target, **kwargs):
    url_val = URL().with_path(target)    
    if kwargs:
        url_val = url_val.with_query({key:value for key, value in kwargs.items() if value})
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
    with ui.row().classes("gap-0") as view:
        [ui.chip(ellipsis_text(text), on_click=lambda text=text: tag_route(text)).props('outline dense') for text in  bean.tags[:MAX_TAGS_PER_BEAN]] if bean.tags else None
    return view
    
def render_bean_title(bean: Bean):
    return ui.label(bean.title).classes("text-bold").style("word-wrap: break-word; overflow-wrap: break-word;")

def render_bean_body(bean: Bean):
    return ui.markdown(bean.summary).style("word-wrap: break-word; overflow-wrap: break-word; text-align: justify;")

def render_bean_stats(bean: Bean, stack: bool): 
    with (ui.column(align_items="stretch").classes(add="gap-0") if stack else ui.row(align_items="baseline")).classes(add="text-caption").style("margin-top: 1px;") as view:   
        ui.label(date_to_str(bean.created or bean.updated))
        with ui.row():
            if bean.comments:
                ui.label(f"💬 {bean.comments}").tooltip(f"{bean.comments} comments across various social media sources")
            if bean.likes:
                ui.label(f"👍 {bean.likes}").tooltip(f"{bean.likes} likes across various social media sources")
    return view
    
def render_text_banner(banner: str):
    with ui.label(banner).classes("text-h5") as view:
        ui.separator().style("margin-top: 5px;")
    return view

def render_expandable_bean(bean: Bean, show_related: bool = True):
    @ui.refreshable
    def render_related_beans(show_items: bool):   
        related_beans, load_beans = ui.state([])
        if show_items and not related_beans:
            load_beans(beanops.related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=MAX_RELATED_ITEMS))     
        render_beans_as_carousel(related_beans, render_whole_bean).set_visibility(show_items)    
    
    CONTENT_STYLE = 'padding: 0px; margin: 0px; word-wrap: break-word; overflow-wrap: break-word;'
    with ui.expansion().props("dense hide-expand-icon") as view:
        with view.add_slot("header"):
            render_bean_banner(bean)

        with ui.element():                        
            render_bean_tags_as_chips(bean)
            render_bean_body(bean)
            with ui.row(align_items="center", wrap=False).classes("text-caption w-full"):
                render_bean_source(bean)
                ui.space()
                related_count = beanops.count_related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=MAX_RELATED_ITEMS+1)
                if show_related and related_count:
                    related_expansion=ui.expansion(
                        text=rounded_number_with_max(related_count, MAX_RELATED_ITEMS)+" related "+ ("stories" if related_count>1 else "story"),
                        on_value_change=lambda: render_related_beans.refresh(related_expansion.value)).props("dense").style("text-align: right; self-align: right;")
            render_related_beans(False)
        ui.query('div.q-expansion-item__header').style(add=CONTENT_STYLE).classes(add="w-full")

    return view

def render_bean_source(bean: Bean):
    return ui.markdown(f"Read more in [{bean.channel or bean.source}]({bean.url})")

def render_bean_banner(bean: Bean):
    with ui.row(wrap=False, align_items="start").classes("w-full") as view:            
        if bean.image_url: 
            with ui.element():   
                ui.image(bean.image_url).classes("w-32 h-32")   
                if is_bean_title_too_long(bean):
                    render_bean_stats(bean, stack=True) 
        with render_bean_title(bean):    
            if not bean.image_url or not is_bean_title_too_long(bean):           
                render_bean_stats(bean, stack=False)    
    return view

def render_whole_bean(bean: Bean):
    with ui.element() as view:
        render_bean_banner(bean)
        render_bean_tags_as_chips(bean)
        render_bean_body(bean)
        ui.markdown(f"Read more in [{bean.channel or bean.source}]({bean.url})").classes("text-caption")
    return view

def render_beans_as_paginated_list(items_count: int, get_beans: Callable):
    # page_index = {"page_index": 1}
    page_count = min(MAX_PAGES, -(-items_count//MAX_ITEMS_PER_PAGE))

    @ui.refreshable
    def render_search_items():
        page, go_to_page = ui.state(0)
        if items_count > MAX_ITEMS_PER_PAGE:
            page_numbers = ui.pagination(min=1, max=page_count, direction_links=True, value=page+1, on_change=lambda: go_to_page(page_numbers.value - 1))
        render_beans_as_list(get_beans(page*MAX_ITEMS_PER_PAGE), True, lambda bean: render_expandable_bean(bean, show_related=False))    
        if items_count > MAX_ITEMS_PER_PAGE:
            ui.pagination(min=1, max=page_count, direction_links=True, value=page+1).bind_value(page_numbers, 'value')

    render_search_items()

def render_beans_as_list(beans, render_articles, bean_render_func):
    with ui.list().props(add="dense" if render_articles else "separator").classes("w-full") as view:        
        for bean in beans:
            with ui.item().classes(bean_item_class(render_articles)).style(bean_item_style):
                bean_render_func(bean)
    return view

def render_beans_as_carousel(beans: list[Bean], bean_render_func):
    with ui.carousel(animated=True, arrows=True).props(f"swipeable control-color=primary").classes("h-full") as view:          
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