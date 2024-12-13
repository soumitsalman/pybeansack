from contextlib import contextmanager
import random
import threading
from typing import Callable
from app.pybeansack.utils import *
from app.pybeansack.datamodels import *
from app.shared.datamodel import *
from app.shared import utils, beanops, espressops
from app.shared.messages import *
from urllib.parse import urlencode
from nicegui import ui, background_tasks, run

MAX_ITEMS_PER_PAGE = 5
MAX_PAGES = 10
MAX_TAGS_PER_BEAN = 5
MAX_RELATED_ITEMS = 5

PRIMARY_COLOR = "#4e392a"
SECONDARY_COLOR = "#b79579"
IMAGE_DIMENSIONS = "w-32 h-28"

CSS_FILE = "./app/web/styles.css"

GOOGLE_ICON = "img:https://www.google.com/favicon.ico"
REDDIT_ICON = "img:https://www.reddit.com/favicon.ico"
LINKEDIN_ICON = "img:https://www.linkedin.com/favicon.ico"
SLACK_ICON = "img:https://slack.com/favicon.ico"
TWITTER_ICON = "img:https://www.x.com/favicon.ico"
WHATSAPP_ICON = "img:/images/whatsapp.png"
ESPRESSO_ICON = "img:/images/favicon.ico"

LOGIN_OPTIONS = [
    {
        "title": "Continue with Google",
        "icon": GOOGLE_ICON,
        "url": "/oauth/google/login"        
    },
    {
        "title": "Continue with Slack",
        "icon": SLACK_ICON,
        "url": "/oauth/slack/login"
    },
    # {
    #     "title": "Continue with LinkedIn",
    #     "icon": LINKEDIN_ICON,
    #     "url": "/oauth/linkedin/login"
    # }
]

reddit_share_url = lambda bean: create_navigation_route("https://www.reddit.com/submit", url=bean.url, title=bean.title, link="LINK")
twitter_share_url = lambda bean: create_navigation_route("https://x.com/intent/tweet", url=bean.url, text=bean.summary)
linkedin_share_url = lambda bean: create_navigation_route("https://www.linkedin.com/shareArticle", url=bean.url, text=bean.title, mini=True)
whatsapp_share_url = lambda bean: create_navigation_route("https://wa.me/", text=f"{bean.title}\n{bean.url}")
slack_share_url = lambda bean: create_navigation_route("https://slack.com/share/url", url=bean.url, text=bean.title)

rounded_number = lambda counter: str(counter) if counter < beanops.MAX_LIMIT else str(beanops.MAX_LIMIT-1)+'+'
rounded_number_with_max = lambda counter, top: str(counter) if counter <= top else str(top)+'+'

def create_navigation_target(base_url, **kwargs):
    if kwargs:
        return base_url+"?"+urlencode(query={key:value for key, value in kwargs.items() if value})
    return base_url

def create_navigation_route(base_url, **kwargs):
    return lambda base_url=base_url, kwargs=kwargs: ui.navigate.to(create_navigation_target(base_url, **kwargs))

def create_barista_route(barista: espressops.Barista):
    return lambda barista=barista: ui.navigate.to(f"/baristas/{barista.id}")

def create_search_target(text):
    return create_navigation_target("/search", q=text) \
        if not utils.is_valid_url(text) else \
            create_navigation_target("/search", url=text)

def render_header(user):
    ui.add_css(CSS_FILE)
    ui.colors(primary=PRIMARY_COLOR, secondary=SECONDARY_COLOR)    
    with ui.header(wrap=False).props("reveal").classes("justify-between items-stretch rounded-borders p-1 q-ma-xs") as header:     
        with ui.button(on_click=create_navigation_route("/")).props("unelevated").classes("q-px-xs"):
            with ui.avatar(square=True, size="md").classes("rounded-borders"):
                ui.image("images/cafecito.png")
            ui.label("Espresso").classes("q-ml-sm")
            
        # TODO: make this pull up side panel
        # bookmarks browse library_books
        ui.button(icon="bookmarks", on_click=create_navigation_route("/baristas")).props("unelevated").classes("lt-sm")
        ui.button(icon="search", on_click=create_navigation_route("/search")).props("unelevated").classes("lt-sm")

        trigger_search = lambda: ui.navigate.to(create_search_target(search_input.value))
        with ui.input(placeholder=SEARCH_PLACEHOLDER) \
            .props('item-aligned clearable dense rounded outlined maxlength=1000 bg-color=dark clear-icon=close') \
            .classes("gt-xs w-1/2 m-0 p-0") \
            .on("keydown.enter", trigger_search) as search_input:    
            prepend = search_input.add_slot("prepend")   
            with prepend:
                ui.button(icon="search", color="secondary", on_click=trigger_search).props("flat rounded").classes("m-0")
                
        (render_user(user) if user else render_login()).props("unelevated").classes("q-mx-xs")
    return header

def render_login():
    with ui.button(icon="login") as view:
        with ui.menu().props("transition-show=jump-down transition-hide=jump-up").classes("max-w-full"):           
            for option in LOGIN_OPTIONS:
                with ui.menu_item(option["title"], on_click=lambda url=option['url']: ui.navigate.to(url)):
                    ui.avatar(option["icon"], color="transparent", square=True)
    return view

def render_user(user: User):
    with ui.button(icon="person").props("round").classes("border-2 border-solid") as view:
        with ui.menu():
            with ui.card():
                if user.image_url:
                    ui.image(user.image_url).classes("w-24")
                ui.label(user.name).classes("text-bold")                   
                with ui.row(wrap=False):        
                    ui.button("Log Out", icon="logout", color="negative", on_click=lambda: ui.navigate.to("/user/me/logout")).props("unelevated size=sm")   
                    # TODO: add other settings options here          
                    # ui.button("Delete Account", icon="cancel", color="negative").props("flat size=sm")
    return view

def render_barista_names(user: User, baristas: list[Barista]):
    with ui.row(align_items="stretch").classes("q-pa-sm") as panel:
        [ui.item(barista.title, on_click=create_barista_route(barista)).classes(f"rounded-borders text-lg bg-primary") for barista in baristas]
    return panel

def render_baristas_panel(user: User):    
    baristas = espressops.db.get_baristas(user.following if user else espressops.DEFAULT_BARISTAS)
    with render_card_container("Following" if user else "Popular Baristas", on_click=create_navigation_route("/baristas")) as panel:        
        [ui.item(barista.title, on_click=create_barista_route(barista)) for barista in baristas]
    return panel

def render_beans(user: User, load_beans: Callable, container: ui.element = None):
    async def render():
        beans = await run.io_bound(load_beans)
        container.clear()
        with container:
            if not beans:
                ui.label(NOTHING_FOUND).classes("w-full text-center") 
            # [render_bean(user, bean, False).classes("w-full") for bean in beans[:MAX_ITEMS_PER_PAGE]] 
            [render_bean_with_related(user, bean).classes("w-full w-full m-0 p-0") for bean in beans] 

    container = container or ui.column(align_items="stretch")
    with container:
        render_skeleton_beans(3)
    background_tasks.create_lazy(render(), name=f"beans-{now()}")
    return container

def render_beans_as_extendable_list(user: User, load_beans: Callable, container: ui.element = None):
    current_start = 0   

    def current_page():
        nonlocal current_start
        beans = load_beans(current_start, MAX_ITEMS_PER_PAGE+1) 
        current_start += MAX_ITEMS_PER_PAGE # moving the cursor
        if len(beans) <= MAX_ITEMS_PER_PAGE:
            more_btn.delete()
        return beans

    async def next_page():
        with disable_button(more_btn):
            beans = await run.io_bound(current_page)   
            with beans_panel:
                # [render_bean(user, bean, False) for bean in beans[:MAX_ITEMS_PER_PAGE]]
                [render_bean_with_related(user, bean).classes("w-full w-full m-0 p-0") for bean in beans[:MAX_ITEMS_PER_PAGE]]

    with ui.column() as view:
        beans_panel = render_beans(user, current_page, container).classes("w-full")
        more_btn = ui.button("More Stories", on_click=next_page).props("icon-right=chevron_right")
    return view  

def render_paginated_beans(user: User, load_beans: Callable, count_items: Callable):    
    @ui.refreshable
    def render(page):
        return render_beans(user, lambda: load_beans((page-1)*MAX_ITEMS_PER_PAGE, MAX_ITEMS_PER_PAGE))        

    with ui.column(align_items="stretch") as panel:
        render(1)
        render_pagination(count_items, lambda page: render.refresh(page))
    return panel

def render_bean_with_related(user: User, bean: Bean):
    with_related_beans = [bean] + beanops.get_related(url=bean.url, tags=None, kinds=None, sources=None, last_ndays=None, start=0, limit=MAX_RELATED_ITEMS)
    return render_swipable_beans(user, with_related_beans)

def render_swipable_beans(user: User, beans: list[Bean]):
    with ui.item() as view:  # Added rounded-borders class here
        with ui.carousel(animated=True, arrows=True).props("swipeable control-color=secondary").classes("rounded-borders w-full h-full"):
            for i, bean in enumerate(beans):
                with ui.carousel_slide(bean.url).classes("w-full m-0 p-0 no-wrap"):  # Added rounded-borders class here
                    render_bean(user, bean, i!=0).classes("w-full m-0 p-0")
    return view

# render_bean = lambda user, bean, expandable: render_expandable_bean(user, bean) if expandable else render_whole_bean(user, bean)
render_bean = lambda user, bean, expanded: render_expandable_bean(user, bean, expanded)

def render_expandable_bean(user: User, bean, expanded: bool = False):
    with ui.expansion(value=expanded).props("dense hide-expand-icon").classes("bg-dark rounded-borders") as expansion:
        header = expansion.add_slot("header")
        with header:
            render_bean_header(user, bean).classes(add="p-0")
        render_bean_body(user, bean)
    return expansion

def render_whole_bean(user: User, bean: Bean):
    with ui.element() as view:
        render_bean_header(user, bean).classes(add="q-mb-sm")
        render_bean_body(user, bean)
    return view 

def render_bean_header(user: User, bean: Bean):
    with ui.row(wrap=False, align_items="stretch").classes("w-full bean-header") as view:            
        if bean.image_url: 
            ui.image(bean.image_url).props("width=8em height=8em")
        with ui.element().classes("w-full"):
            ui.label(bean.title).classes("bean-title")                
            render_bean_stats(user, bean).classes("text-caption") 
    return view

def render_bean_stats(user: User, bean: Bean): 
    with ui.row(align_items="stretch").classes("w-full") as view:       
        ui.label(f"{beanops.naturalday(bean.created or bean.updated)}'s {bean.kind}")
        if bean.comments:
            ui.label(f"💬 {bean.comments}").tooltip(f"{bean.comments} comments across various social media sources")
        if bean.likes:
            ui.label(f"👍 {bean.likes}").tooltip(f"{bean.likes} likes across various social media sources")
        if bean.shares and bean.shares > 1:
            ui.label(f"🔗 {bean.shares}").tooltip(f"{bean.shares} shares across various social media sources") # another option 🗞️
    return view

def render_bean_body(user, bean):
    with ui.column(align_items="stretch").classes("w-full m-0 p-0") as view:
        if bean.tags:
            render_bean_tags(bean)
        if bean.summary:
            ui.markdown(bean.summary).classes("bean-body")
        with ui.row(wrap=False, align_items="stretch").classes("w-full justify-between p-0 m-0"):
            render_bean_source(bean).classes("text-caption bean-source")
            render_bean_actions(user, bean)
    return view

def render_bean_tags(bean: Bean):
    # make_tag = lambda tag: ui.chip(tag, color="secondary", on_click=create_navigation_route("/search", tag=tag)).props('outline dense').classes("tag tag-space")
    make_tag = lambda tag: ui.link(tag, target=create_navigation_target("/beans", tag=tag)).classes("tag q-mr-md").style("color: secondary; text-decoration: none;")
    with ui.row(wrap=True, align_items="baseline").classes("w-full gap-0 m-0 p-0 text-caption") as view:
        [make_tag(tag) for tag in random.sample(bean.tags, min(MAX_TAGS_PER_BEAN, len(bean.tags)))]
    return view

def render_bean_source(bean: Bean):
    with ui.row(wrap=False, align_items="center").classes("gap-0") as view:        
        ui.avatar("img:"+beanops.favicon(bean), size="xs", color="transparent")
        ui.link(bean.source, bean.url, new_tab=True).classes("ellipsis-30")
    return view

def render_bean_actions(user: User, bean: Bean):
    # publish = lambda: ui.notify(PUBLISHED if espressops.publish(user, bean.url) else UNKNOWN_ERROR)
    share_button = lambda url_func, icon: ui.button(on_click=url_func(bean), icon=icon).props("flat")
    with ui.button_group().props("flat size=sm").classes("p-0 m-0"):
        ui.button(icon="more", color="secondary", on_click=create_navigation_route("/search", q=bean.url)).props("flat size=sm").tooltip("More like this")
        with ui.button(icon="share", color="secondary").props("flat size=sm") as view:
            with ui.menu():
                with ui.row(wrap=False, align_items="stretch").classes("gap-1 m-0 p-0"):
                    share_button(reddit_share_url, REDDIT_ICON).tooltip("Share on Reddit")
                    share_button(linkedin_share_url, LINKEDIN_ICON).tooltip("Share on LinkedIn")
                    share_button(twitter_share_url, TWITTER_ICON).tooltip("Share on X")
                    share_button(whatsapp_share_url, WHATSAPP_ICON).tooltip("Share on WhatsApp")
                    # share_button(slack_share_url, SLACK_ICON).tooltip("Share on Slack") 
                    # if user:
                    #     ui.button(on_click=publish, icon=ESPRESSO_ICON).props("flat").tooltip("Publish on Espresso")
    return view  

def render_filter_tags(load_tags: Callable, on_selection_changed: Callable):
    selected_tags = []
    def change_tag_selection(tag: str, selected: bool):        
        selected_tags.append(tag) if selected else selected_tags.remove(tag)
        on_selection_changed(selected_tags) 

    async def render():
        beans = await run.io_bound(load_tags)
        if beans:
            holder.clear()
            with holder:
                [ui.chip(
                    bean.tags, 
                    selectable=True, 
                    color="dark", 
                    on_selection_change=lambda e: change_tag_selection(e.sender.text, e.sender.selected)).props("flat filled").classes(" h-full") for bean in beans]
        else:
            holder.delete() 

    # with ui.scroll_area().classes("w-full h-20 p-0 m-0") as view:
    with ui.row(align_items="stretch").classes("gap-0 p-0 m-0") as holder:
        ui.skeleton("rect", width="100%").classes("w-full h-full")
    background_tasks.create_lazy(render(), name=f"tags-{now()}")
    # return view
    return holder

def render_pagination(count_items: Callable, on_change: Callable):
    async def render():
        items_count = await run.io_bound(count_items)
        page_count = -(-items_count//MAX_ITEMS_PER_PAGE)
        view.clear()
        if items_count > MAX_ITEMS_PER_PAGE:
            with view:
                ui.pagination(min=1, max=page_count, direction_links=True, on_change=lambda e: on_change(e.sender.value)).props("max-pages=10 ellipses")            

    with ui.element() as view:
        ui.skeleton("rect", width="100%").classes("w-full")
    background_tasks.create_lazy(render(), name=f"pagination-{now()}")
    return view

def render_skeleton_beans(count = 3):
    skeletons = []
    for _ in range(count):
        with ui.item().classes("w-full") as item:
            with ui.item_section().props("side"):
                ui.skeleton("rect", size="8em")
            with ui.item_section().props("top"):
                ui.skeleton("text", width="100%")
                ui.skeleton("text", width="100%")
                ui.skeleton("text", width="40%")
        skeletons.append(item)
    return skeletons

def render_skeleton_baristas(count = 3):
    skeletons = []
    for _ in range(count):
        with ui.item().classes("w-full") as item:
            with ui.item_section().props("side"):
                ui.skeleton("rect", size="8em")
            with ui.item_section().props("top"):
                ui.skeleton("text", width="40%")
                ui.skeleton("text", width="100%")    
        skeletons.append(item)
    return skeletons

def render_footer():
    ui.separator().style("height: 5px;").classes("w-full")
    text = "[[Terms of Use](https://github.com/soumitsalman/espresso/blob/main/docs/terms-of-use.md)]   [[Privacy Policy](https://github.com/soumitsalman/espresso/blob/main/docs/privacy-policy.md)]   [[Espresso](https://github.com/soumitsalman/espresso/blob/main/README.md)]   [[Project Cafecito](https://github.com/soumitsalman/espresso/blob/main/docs/project-cafecito.md)]\n\nCopyright © 2024 Project Cafecito. All rights reserved."
    return ui.markdown(text).classes("w-full text-caption text-center")

def render_error_text(msg: str):
    return ui.label(msg).classes("self-center text-center")

def render_card_container(label: str, on_click: Callable = None, header_classes: str = "text-h6"):
    with ui.card(align_items="stretch").tight().props("flat") as panel:        
        holder = ui.item(label, on_click=on_click).classes(header_classes)
        if on_click:
            holder.props("clickable").tooltip("Click for more")
        ui.separator().classes("q-mb-xs") 
    return panel

@contextmanager
def disable_button(button: ui.button):
    button.disable()
    button.props(add="loading")
    try:
        yield
    finally:
        button.props(remove="loading")
        button.enable()

def debounce(func, wait):
    last_call = None
    def debounced(*args, **kwargs):
        nonlocal last_call
        if last_call:
            last_call.cancel()
        last_call = threading.Timer(wait, func, args, kwargs)
        last_call.start()
    return debounced
