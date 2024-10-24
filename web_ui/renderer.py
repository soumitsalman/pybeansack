from contextlib import contextmanager
from pybeansack.datamodels import *
from shared import beanops, config, espressops
from web_ui.custom_ui import *
from nicegui import ui
from icecream import ic
from urllib.parse import urlencode
from shared.messages import *

#b59475 → #604934 (Light Beige/Brown)
#b49374 → #604933 (Beige/Brown)
#9d7456 → #4e392a (Deep Coffee Brown)
#b79579 → #624935 (Warm Beige)
#b69478 → #624934 (Soft Brown) 

PRIMARY_COLOR = "#4e392a"
SECONDARY_COLOR = "#b79579"

# themes
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Quicksand&display=swap');
@import url('https://fonts.googleapis.com/css2?family=Bree+Serif&display=swap');
@import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200')
    
body {
    font-family: 'Quicksand', serif;
    font-style: normal; 
    color: #BBBBBB;
}

.text-caption { color: #999999; }

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

IMAGE_DIMENSIONS = "w-32 h-24"

REDDIT_ICON_URL = "img:https://www.reddit.com/favicon.ico"
LINKEDIN_ICON_URL = "img:https://www.linkedin.com/favicon.ico"
SLACK_ICON_URL = "img:https://www.slack.com/favicon.ico"
TWITTER_ICON_URL = "img:https://www.x.com/favicon.ico"
WHATSAPP_ICON_URL = "img:https://www.whatsapp.com/favicon.ico"
ESPRESSO_ICON_URL = "img:/images/favicon.jpg"

go_to = lambda url: ui.navigate.to(url, new_tab=True)
reddit_share_url = lambda bean: make_navigation_target("https://www.reddit.com/submit", url=bean.url, title=bean.title, link="LINK")
twitter_share_url = lambda bean: make_navigation_target("https://x.com/intent/tweet", url=bean.url, text=bean.title)
linkedin_share_url = lambda bean: make_navigation_target("https://www.linkedin.com/shareArticle", url=bean.url, text=bean.title, source=config.host_url(), mini=True)
whatsapp_share_url = lambda bean: make_navigation_target("https://wa.me/", text=f"{bean.title}\n{bean.url}")
slack_share_url = lambda bean: make_navigation_target("https://slack.com/share/url", url=bean.url, text=bean.title)

bean_item_class = lambda is_article: "w-full border-[1px]" if is_article else "w-full"
bean_item_style = "border-radius: 5px; margin-bottom: 5px; padding: 0px;"
tag_route = lambda tag: ui.navigate.to(make_navigation_target("/search", tag=tag))
ellipsis_text = lambda text, cap: text if len(text)<=cap else f"{text[:cap-3]}..."
is_bean_title_too_long = lambda bean: len(bean.title) >= 175

def make_navigation_target(base_url, **kwargs):
    if kwargs:
        return base_url+"?"+urlencode(query={key:value for key, value in kwargs.items() if value})
    return base_url

def render_settings_as_text(settings: dict):
    return ui.markdown("Currently showing news, blogs and social media posts on %s." % (", ".join([f"**{espressops.category_label(topic)}**" for topic in settings['topics']])))

def render_separator():
    return ui.separator().style("height: 5px;").classes("w-full m-0 p-0 gap-0")

def render_banner_text(banner: str):
    with ui.label(banner).classes("text-h5") as view:
        ui.separator().style("margin-top: 5px;")
    return view

def render_error_text(msg: str):
    return ui.label(msg).classes("text-h5 self-center text-center")

def render_footer_text():
    text = "[[Terms of Use](/doc/terms-of-use)]   [[Privacy Policy](/doc/privacy-policy)]   [[Espresso](/doc/espresso)]   [[Project Cafecito](/doc/project-cafecito)]\n\nCopyright © 2024 Project Cafecito. All rights reserved."
    return ui.markdown(text).classes("w-full text-caption text-center")

def render_header():
    ui.colors(primary=PRIMARY_COLOR, secondary=SECONDARY_COLOR)
    ui.add_css(content=CSS)
    with ui.header().classes(replace="row", add="header-container").classes("w-full") as header:     
        ui.image("images/cafecito.png").props("width=3rem height=3rem")
    return header

def render_tags_as_chips(tags: list[str], on_click: Callable = None, on_select: Callable = None):
    async def on_selection_changed(sender):
        sender.props(remove="outline") if sender.selected else sender.props(add="outline")
        await on_select(sender.text, sender.selected)

    if tags:
        return [
            ui.chip(
                ellipsis_text(tag, 30), 
                color="secondary",
                on_click=(lambda e: on_click(e.sender.text)) if on_click else None,
                selectable=bool(on_select),
                on_selection_change=(lambda e: on_selection_changed(e.sender)) if on_select else None).props('outline dense') \
            for tag in tags]
            
def render_bean_tags(bean: Bean):
    if bean.tags:
        with ui.row(align_items="center").classes("gap-0") as view:
            render_tags_as_chips([tag for tag in bean.tags[:MAX_TAGS_PER_BEAN]], on_click=tag_route)
        return view
    
def render_bean_title(bean: Bean):
    return ui.label(bean.title).classes("bean-header")

def render_bean_body(bean: Bean):
    if bean.summary:
        return ui.markdown(ellipsis_text(bean.summary, 1500)).style("word-wrap: break-word; overflow-wrap: break-word; text-align: justify;").tooltip("AI generated (duh!)")

def render_bean_stats(bean: Bean, stack: bool): 
    with (ui.column(align_items="stretch").classes(add="gap-0") if stack else ui.row(align_items="baseline")).classes(add="text-caption").style("margin-top: 1px;") as view:  
        ui.label(beanops.naturalday(bean.created or bean.updated))
        with ui.row():
            if bean.comments:
                ui.label(f"💬 {bean.comments}").tooltip(f"{bean.comments} comments across various social media sources")
            if bean.likes:
                ui.label(f"👍 {bean.likes}").tooltip(f"{bean.likes} likes across various social media sources")
    return view

def render_bean_source(bean: Bean):
    with ui.row(wrap=False, align_items="center").classes("gap-0") as view:
        ui.avatar("img:"+beanops.favicon(bean), size="xs", color="transparent")
        ui.link(ellipsis_text(bean.source, 30), bean.url, new_tab=True)
    return view

def render_bean_banner(bean: Bean):
    with ui.row(wrap=False, align_items="start").classes("w-full") as view:            
        if bean.image_url: 
            with ui.element():   
                ui.image(bean.image_url).classes(IMAGE_DIMENSIONS)   
                render_bean_stats(bean, stack=True) 
        with ui.element().classes("w-full"):
            render_bean_title(bean) 
            if not bean.image_url:        
                render_bean_stats(bean, stack=False)    
    return view

async def publish(user, bean: Bean):
    msg = LOGIN_FIRST
    if user:
        msg = PUBLISHED if espressops.publish(user, bean.url) else UNKNOWN_ERROR
    ui.notify(msg)

def render_bean_shares(user, bean: Bean):
    share_button = lambda url_func, icon: ui.button(on_click=lambda: go_to(url_func(bean)), icon=icon).props("flat")
    with ui.button(icon="share") as view:
        with ui.menu():
            with ui.row(wrap=False, align_items="stretch").classes("gap-0 m-0 p-0"):
                share_button(reddit_share_url, REDDIT_ICON_URL).tooltip("Share on Reddit")
                share_button(linkedin_share_url, LINKEDIN_ICON_URL).tooltip("Share on LinkedIn")
                share_button(twitter_share_url, TWITTER_ICON_URL).tooltip("Share on X")
                share_button(whatsapp_share_url, WHATSAPP_ICON_URL).tooltip("Share on WhatsApp")
                # share_button(slack_share_url, SLACK_ICON_URL).tooltip("Share on Slack") 
                ui.button(on_click=lambda: publish(user, bean), icon=ESPRESSO_ICON_URL).props("flat").tooltip("Publish on Espresso")
    return view

def render_bean_actions(user, bean: Bean, show_related_items: Callable = None):
    related_count = beanops.count_related(cluster_id=bean.cluster_id, url=bean.url, limit=MAX_RELATED_ITEMS+1)

    ACTION_BUTTON_PROPS = f"flat size=sm color=secondary"
    with ui.row(align_items="center", wrap=False).classes("text-caption w-full"):
        render_bean_source(bean)
        ui.space()
        with ui.button_group().props(f"unelevated dense flat"):  
            render_bean_shares(user, bean).props(ACTION_BUTTON_PROPS)
            if show_related_items and related_count:
                with ExpandButton().on_click(lambda e: show_related_items(e.sender.value)).props(ACTION_BUTTON_PROPS):
                    ui.badge(rounded_number_with_max(related_count, 5)).props("transparent")

def render_whole_bean(user, bean: Bean):
    with ui.element() as view:
        render_bean_banner(bean)
        render_bean_tags(bean)
        render_bean_body(bean)
        render_bean_actions(user, bean, None)
    return view

def render_expandable_bean(user, bean: Bean, show_related: bool = True):
    @ui.refreshable
    def render_related_beans(show_items: bool):   
        related_beans, load_beans = ui.state([])
        if show_items and not related_beans:
            load_beans(beanops.related(cluster_id=bean.cluster_id, url=bean.url, limit=MAX_RELATED_ITEMS))     
        render_beans_as_carousel(related_beans, lambda bean: render_whole_bean(user, bean)).set_visibility(show_items)    
    
    CONTENT_STYLE = 'padding: 0px; margin: 0px; word-wrap: break-word; overflow-wrap: break-word;'
    with ui.expansion().props("dense hide-expand-icon").classes("w-full") as view:
        with view.add_slot("header"):
            render_bean_banner(bean)

        with ui.element().classes("w-full"):                        
            render_bean_tags(bean)
            render_bean_body(bean)
            render_bean_actions(user, bean, render_related_beans.refresh if show_related else None)
            if show_related:
                render_related_beans(False)
        ui.query('div.q-expansion-item__header').style(add=CONTENT_STYLE).classes(add="w-full")

    return view

def render_beans_as_paginated_list(get_beans: Callable, items_count: int, bean_render_func: Callable):
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
    with ui.carousel(animated=True, arrows=True).props("swipeable vertical control-color=secondary").classes("h-full rounded-borders").style("background-color: #333333;") as view:          
        for bean in beans:
            with ui.carousel_slide(name=bean.url).classes("column no-wrap"):
                bean_render_func(bean)
    return view

def render_skeleton_tags(count = 3):    
    return [ui.skeleton("QChip").props("outline") for _ in range(count)]

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

@contextmanager
def disable_button(button: ui.button):
    button.disable()
    try:
        yield
    finally:
        button.enable()