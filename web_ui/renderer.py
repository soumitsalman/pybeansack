from contextlib import contextmanager
import random
import threading
from pybeansack.datamodels import *
from shared import beanops, espressops, utils
from web_ui.custom_ui import *
from nicegui import ui, background_tasks, run
from icecream import ic
from urllib.parse import urlencode
from shared.messages import *

PRIMARY_COLOR = "#4e392a"
SECONDARY_COLOR = "#b79579"
CSS_FILE = "./web_ui/styles.css"
IMAGE_DIMENSIONS = "w-32 h-28"
GOOGLE_ANALYTICS_SCRIPT = '''
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-NBSTNYWPG1"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-NBSTNYWPG1');
</script>
'''

REDDIT_ICON_URL = "img:https://www.reddit.com/favicon.ico"
LINKEDIN_ICON_URL = "img:https://www.linkedin.com/favicon.ico"
SLACK_ICON_URL = "img:https://www.slack.com/favicon.ico"
TWITTER_ICON_URL = "img:https://www.x.com/favicon.ico"
WHATSAPP_ICON_URL = "img:/images/whatsapp.png"
ESPRESSO_ICON_URL = "img:/images/favicon.jpg"

reddit_share_url = lambda bean: create_navigation_route("https://www.reddit.com/submit", url=bean.url, title=bean.title, link="LINK")
twitter_share_url = lambda bean: create_navigation_route("https://x.com/intent/tweet", url=bean.url, text=bean.summary)
linkedin_share_url = lambda bean: create_navigation_route("https://www.linkedin.com/shareArticle", url=bean.url, text=bean.title, mini=True)
whatsapp_share_url = lambda bean: create_navigation_route("https://wa.me/", text=f"{bean.title}\n{bean.url}")
slack_share_url = lambda bean: create_navigation_route("https://slack.com/share/url", url=bean.url, text=bean.title)

rounded_number = lambda counter: str(counter) if counter < MAX_LIMIT else str(MAX_LIMIT-1)+'+'
rounded_number_with_max = lambda counter, top: str(counter) if counter <= top else str(top)+'+'

def create_navigation_target(base_url, **kwargs):
    if kwargs:
        return base_url+"?"+urlencode(query={key:value for key, value in kwargs.items() if value})
    return base_url

def create_navigation_route(base_url, **kwargs):
    return lambda base_url=base_url, kwargs=kwargs: ui.navigate.to(create_navigation_target(base_url, **kwargs))

def create_barista_route(barista: espressops.Barista):
    return lambda barista=barista: ui.navigate.to(f"/trending/{barista.id}")

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
            
        ui.button(icon="trending_up", on_click=create_navigation_route("/trending")).props("unelevated").classes("lt-sm")
        ui.button(icon="search", on_click=create_navigation_route("/search")).props("unelevated").classes("lt-sm")

        trigger_search = lambda: ui.navigate.to(create_search_target(search_input.value))
        with ui.input(placeholder=SEARCH_PLACEHOLDER) \
            .props('item-aligned clearable dense rounded outlined maxlength=1000 bg-color=dark clear-icon=close') \
            .classes("gt-xs w-1/2 m-0 p-0") \
            .on("keydown.enter", trigger_search) as search_input:    
            prepend = search_input.add_slot("prepend")   
            with prepend:
                ui.button(icon="search", color="secondary", on_click=trigger_search).props("flat rounded").classes("m-0")
                
        ui.button(icon="person" if user else "login", on_click=lambda: ui.notify("Coming Soon")).props("unelevated").tooltip("Coming Soon")
        # with ui.button(icon="person" if user else "login").props("unelevated"):
        #     (render_user_card(user) if user else render_login_buttons())
    return header

def render_user_card(user):
    with ui.menu() as menu:
        with ui.card(align_items="stretch"):
            with ui.item("john-doe"): 
                with ui.item_section():
                    ui.link(user[K_ID], target=f"//{user[K_ID]}")           
                with ui.item_section().props("avatar"):
                    (ui.image(user['image_url']) if ('image_url' in user) else ui.avatar(icon="person", square=True)).classes("rounded-borders")
            with ui.row(wrap=False):
                ui.button("Sign Out", icon="logout", color="negative").props("unelevated size=sm")
                ui.button("Delete Account", icon="cancel", color="negative").props("flat size=sm")
    return menu

def render_login_buttons():
    with ui.menu() as menu:                       
        with ui.menu_item(text="Continue with Reddit", on_click=lambda: ui.navigate.to("/reddit/login")).classes("rounded-borders bg-color=#FF4500;"):
            ui.avatar(REDDIT_ICON_URL, color="transparent")
        with ui.menu_item(text="Continue with Slack", on_click=lambda: ui.navigate.to('/web/slack/login')).classes("rounded-borders bg-color=#8E44AD;"):
            ui.avatar(SLACK_ICON_URL, color="transparent")
    return menu

def render_barista_names(user, baristas: list[espressops.Barista]):
    with ui.row(align_items="stretch").classes("q-pa-sm") as panel:
        [ui.item(barista.title, on_click=create_barista_route(barista)).classes(f"rounded-borders text-lg bg-primary") for barista in baristas]
    return panel

def render_baristas_panel(user):
    baristas = espressops.get_following_baristas(user) or espressops.get_baristas(utils.DEFAULT_BARISTAS)
    with render_card_container("Following" if user else "Popular Baristas", on_click=create_navigation_route("/trending")) as panel:        
        [ui.item(barista.title, on_click=create_barista_route(barista)) for barista in baristas]
    return panel

def render_beans(user, load_beans: Callable, skeleton_count: int = 3):
    async def render():
        beans = await run.io_bound(load_beans)
        view.clear()
        with view:
            [render_bean_with_related(user, bean).classes("w-full w-full q-mb-sm p-0") for bean in beans] \
                if beans else ui.label(NOTHING_FOUND).classes("w-full text-center")

    with ui.list() as view:
        render_skeleton_beans(skeleton_count).classes("w-full")
    background_tasks.create_lazy(render(), name=f"beans-{utils.now()}")
    return view

def render_beans_as_extendable_list(user, load_beans: Callable, skeleton_count: int = 3):
    start = 0   
    async def render():
        with disable_button(more_btn):
            nonlocal start
            beans = await run.io_bound(load_beans, start, MAX_ITEMS_PER_PAGE+1)              
            if start == 0:
                holder.clear()
            with holder:
                if not beans and not start:
                    ui.label(NOTHING_FOUND).classes("w-full text-center")
                [render_bean_with_related(user, bean).classes("w-full w-full q-mb-sm p-0") for bean in beans[:MAX_ITEMS_PER_PAGE]]
                    
        # if there are no more beans, delete the more button
        start += MAX_ITEMS_PER_PAGE
        if len(beans) <= MAX_ITEMS_PER_PAGE:
            more_btn.delete()

    with ui.column(align_items="start") as view:
        with ui.list().classes("w-full m-0 p-0") as holder:
            render_skeleton_beans(skeleton_count)
        more_btn = ui.button("More Stories", on_click=render).props("icon-right=chevron_right").classes("q-mx-sm")
    background_tasks.create_lazy(render(), name=f"appendable-beans-{start}-{utils.now()}")
    return view  

def render_paginated_beans(user, load_beans: Callable, get_items_count: Callable):
    async def render():
        beans_panel.clear()
        with beans_panel:
            render_skeleton_beans(2)

        page = pagination_panel.value - 1
        beans = await run.io_bound(load_beans, page*MAX_ITEMS_PER_PAGE, MAX_ITEMS_PER_PAGE)    

        beans_panel.clear()
        with beans_panel:
            [render_bean_with_related(user, bean).classes("w-full w-full q-mb-sm p-0") for bean in beans] \
                if beans else ui.label(NOTHING_FOUND).classes("w-full text-center")

    async def render_pagination():
        with panel:
            pagination_skeleton = ui.skeleton("rect", width="100%").classes("w-full")

        items_count = await run.io_bound(get_items_count)
        page_count = -(-items_count//MAX_ITEMS_PER_PAGE)

        pagination_skeleton.delete()
        if items_count > MAX_ITEMS_PER_PAGE:
            pagination_panel.props(f"max={page_count}").set_visibility(True)
        else:
            pagination_panel.delete()

    with ui.column(align_items="stretch") as panel:
        beans_panel = ui.list().classes("w-full")
        pagination_panel = ui.pagination(min=1, max=5, direction_links=True, on_change=render).props("max-pages=7 ellipses")
        pagination_panel.set_visibility(False)

    background_tasks.create_lazy(render(), name=f"paginated-beans-{utils.now()}")
    background_tasks.create_lazy(render_pagination(), name=f"pagination-numbers-{utils.now()}")

    return panel

def render_bean_with_related(user, bean: Bean):
    with_related_beans = [bean] + beanops.get_related(url=bean.url, tags=None, kinds=bean.kind, sources=None, last_ndays=None, start=0, limit=MAX_RELATED_ITEMS)
    return render_swipable_beans(user, with_related_beans)

def render_swipable_beans(user, beans: list[Bean]):
    with ui.item() as view:  # Added rounded-borders class here
        with ui.carousel(animated=True, arrows=True).props("swipeable control-color=secondary").classes("rounded-borders bg-grey-10 w-full h-full"):
            for i, bean in enumerate(beans):
                with ui.carousel_slide(bean.url).classes("w-full m-0 q-pa-sm no-wrap"):  # Added rounded-borders class here
                    render_bean(user, bean, i!=0).classes("w-full")
    return view

# render_bean = lambda user, bean, expandable: render_expandable_bean(user, bean) if expandable else render_whole_bean(user, bean)
render_bean = lambda user, bean, expanded: render_expandable_bean(user, bean, expanded)

def render_expandable_bean(user, bean, expanded: bool = False):
    with ui.expansion(value=expanded).props("dense hide-expand-icon").classes("bean-expansion") as expansion:
        header = expansion.add_slot("header")
        with header:
            render_bean_header(user, bean).classes(add="p-0")
        render_bean_body(user, bean)
    return expansion

def render_whole_bean(user, bean: Bean):
    with ui.element() as view:
        render_bean_header(user, bean).classes(add="q-mb-sm")
        render_bean_body(user, bean)
    return view 

def render_bean_header(user: dict, bean: Bean):
    with ui.row(wrap=False, align_items="stretch").classes("w-full bean-header") as view:            
        if bean.image_url: 
            ui.image(bean.image_url).props("width=8em height=8em")
        with ui.element().classes("w-full"):
            ui.label(bean.title).classes("bean-title")                
            render_bean_stats(user, bean).classes("text-caption") 
    return view

def render_bean_stats(user, bean: Bean): 
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
    make_tag = lambda tag: ui.link(tag, target=create_navigation_target("/trending", tag=tag)).classes("tag q-mr-md").style("color: secondary; text-decoration: none;")
    with ui.row(wrap=True, align_items="baseline").classes("w-full gap-0 m-0 p-0 text-caption") as view:
        [make_tag(tag) for tag in random.sample(bean.tags, min(MAX_TAGS_PER_BEAN, len(bean.tags)))]
    return view

def render_bean_source(bean: Bean):
    with ui.row(wrap=False, align_items="center").classes("gap-0") as view:        
        ui.avatar("img:"+beanops.favicon(bean), size="xs", color="transparent")
        ui.link(bean.source, bean.url, new_tab=True).classes("ellipsis-30")
    return view

def render_bean_actions(user, bean: Bean):
    publish = lambda: ui.notify(PUBLISHED if espressops.publish(user, bean.url) else UNKNOWN_ERROR)
    share_button = lambda url_func, icon: ui.button(on_click=url_func(bean), icon=icon).props("flat")   

    with ui.button_group().props("flat size=sm").classes("p-0 m-0"):
        ui.button(icon="more", color="secondary", on_click=create_navigation_route("/search", q=bean.url)).props("flat size=sm").tooltip("More like this")
        with ui.button(icon="share", color="secondary").props("flat size=sm") as view:
            with ui.menu():
                with ui.row(wrap=False, align_items="stretch").classes("gap-1 m-0 p-0"):
                    share_button(reddit_share_url, REDDIT_ICON_URL).tooltip("Share on Reddit")
                    share_button(linkedin_share_url, LINKEDIN_ICON_URL).tooltip("Share on LinkedIn")
                    share_button(twitter_share_url, TWITTER_ICON_URL).tooltip("Share on X")
                    share_button(whatsapp_share_url, WHATSAPP_ICON_URL).tooltip("Share on WhatsApp")
                    # share_button(slack_share_url, SLACK_ICON_URL).tooltip("Share on Slack") 
                    if user:
                        ui.button(on_click=publish, icon=ESPRESSO_ICON_URL).props("flat").tooltip("Publish on Espresso")
    return view  

def render_tags_to_filter(load_tags: Callable, on_selection_changed: Callable):
    async def render():
        beans = await run.io_bound(load_tags)
        if beans:
            holder.clear()
            with holder:
                [ui.chip(bean.tags, selectable=True, color="secondary", on_selection_change=lambda e: on_selection_changed(e.sender.text, e.sender.selected)).props("outline dense") for bean in beans]
        else:
            holder.delete() 

    with ui.row(align_items="stretch").classes("gap-0 bg-dark rounded-borders q-px-xs") as holder:
        ui.skeleton("rect", width="100%").classes("w-full")
    background_tasks.create_lazy(render(), name=f"tags-{utils.now()}")
    return holder

def render_skeleton_beans(count = 3):
    with ui.list() as holder:
        for _ in range(count):
            with ui.item():
                with ui.item_section().props("side"):
                    ui.skeleton("rect", size="8em")
                with ui.item_section().props("top"):
                    ui.skeleton("text", width="100%")
                    ui.skeleton("text", width="100%")
                    ui.skeleton("text", width="40%")
                           
    return holder 

def render_skeleton_baristas(count = 3):
    with ui.list() as holder:
        for _ in range(count):
            with ui.item():
                with ui.item_section().props("side"):
                    ui.skeleton("rect", size="8em")
                with ui.item_section().props("top"):
                    ui.skeleton("text", width="40%")
                    ui.skeleton("text", width="100%")    
    return holder 

def render_footer():
    ui.separator().style("height: 5px;").classes("w-full")
    text = "[[Terms of Use](/docs/terms-of-use.md)]   [[Privacy Policy](/docs/privacy-policy.md)]   [[Espresso](/docs/espresso.md)]   [[Project Cafecito](/docs/project-cafecito.md)]\n\nCopyright © 2024 Project Cafecito. All rights reserved."
    return ui.markdown(text).classes("w-full text-caption text-center")

def render_error_text(msg: str):
    return ui.label(msg).classes("self-center text-center")

def render_card_container(label: str, on_click: Callable = None, header_classes: str = "text-h6"):
    with ui.card(align_items="stretch").tight().props("flat") as panel:        
        holder = ui.item(label, on_click=on_click).classes(header_classes)
        if on_click:
            holder.props("clickable").tooltip("Click for more")
        ui.separator() 
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

# def render_tags_as_chips(tags: list[str], on_click: Callable = None, on_select: Callable = None):
#     async def on_selection_changed(sender):
#         sender.props(remove="outline") if sender.selected else sender.props(add="outline")
#         await on_select(sender.text, sender.selected)

#     if tags:
#         return [
#             ui.chip(
#                 ellipsis_text(tag, 30), 
#                 color="secondary",
#                 on_click=(lambda e: on_click(e.sender.text)) if on_click else None,
#                 selectable=bool(on_select),
#                 on_selection_change=(lambda e: on_selection_changed(e.sender)) if on_select else None).props('outline dense') \
#             for tag in tags]
            

    
# def render_bean_title(bean: Bean):
#     return ui.label(bean.title).classes("bean-header")

# def render_bean_body(bean: Bean):
#     if bean.summary:
#         return ui.markdown(ellipsis_text(bean.summary, 1500)).style("word-wrap: break-word; overflow-wrap: break-word; text-align: justify;").tooltip("AI generated (duh!)")

# def render_bean_stats(bean: Bean, stack: bool): 
#     with (ui.column(align_items="stretch").classes(add="gap-0") if stack else ui.row(align_items="baseline")).classes(add="text-caption").style("margin-top: 1px;") as view:  
#         ui.label(beanops.naturalday(bean.created or bean.updated))
#         with ui.row():
#             if bean.comments:
#                 ui.label(f"💬 {bean.comments}").tooltip(f"{bean.comments} comments across various social media sources")
#             if bean.likes:
#                 ui.label(f"👍 {bean.likes}").tooltip(f"{bean.likes} likes across various social media sources")
#     return view

# def render_bean_source(bean: Bean):
#     with ui.row(wrap=False, align_items="center").classes("gap-0") as view:
#         ui.avatar("img:"+beanops.favicon(bean), size="xs", color="transparent")
#         ui.link(ellipsis_text(bean.source, 30), bean.url, new_tab=True)
#     return view





# def render_bean_shares(user, bean: Bean):
#     share_button = lambda url_func, icon: ui.button(on_click=lambda: go_to(url_func(bean)), icon=icon).props("flat")
#     with ui.button(icon="share") as view:
#         with ui.menu():
#             with ui.row(wrap=False, align_items="stretch").classes("m-0 p-0"):
#                 share_button(reddit_share_url, REDDIT_ICON_URL).tooltip("Share on Reddit")
#                 share_button(linkedin_share_url, LINKEDIN_ICON_URL).tooltip("Share on LinkedIn")
#                 share_button(twitter_share_url, TWITTER_ICON_URL).tooltip("Share on X")
#                 share_button(whatsapp_share_url, WHATSAPP_ICON_URL).tooltip("Share on WhatsApp")
#                 # share_button(slack_share_url, SLACK_ICON_URL).tooltip("Share on Slack") 
#                 ui.button(on_click=lambda: publish(user, bean), icon=ESPRESSO_ICON_URL).props("flat").tooltip("Publish on Espresso")
#     return view

# def render_bean_actions(user, bean: Bean, show_related_items: Callable = None):
#     related_count = beanops.count_related_beans(cluster_id=bean.cluster_id, url=bean.url, limit=MAX_RELATED_ITEMS+1)

#     ACTION_BUTTON_PROPS = f"flat size=sm color=secondary"
#     with ui.row(align_items="center", wrap=False).classes("text-caption w-full"):
#         render_bean_source(bean)
#         ui.space()
#         with ui.button_group().props(f"unelevated dense flat"):  
#             render_bean_shares(user, bean).props(ACTION_BUTTON_PROPS)
#             if show_related_items and related_count:
#                 with ExpandButton().on_click(lambda e: show_related_items(e.sender.value)).props(ACTION_BUTTON_PROPS):
#                     ui.badge(rounded_number_with_max(related_count, 5)).props("transparent")



# def render_expandable_bean(user, bean: Bean, show_related: bool = True):
#     @ui.refreshable
#     def render_related_beans(show_items: bool):   
#         related_beans, load_beans = ui.state([])
#         if show_items and not related_beans:
#             load_beans(beanops.get_related(url=bean.url, tags=None, kinds=None, sources=None, last_ndays=None, start=0, limit=MAX_RELATED_ITEMS))     
#         render_beans_as_carousel(related_beans, lambda bean: render_whole_bean(user, bean)).set_visibility(show_items)    
    
#     CONTENT_STYLE = 'padding: 0px; margin: 0px; word-wrap: break-word; overflow-wrap: break-word;'
#     with ui.expansion().props("dense hide-expand-icon").classes("w-full") as view:
#         with view.add_slot("header"):
#             render_bean_banner(bean)

#         with ui.element().classes("w-full"):                        
#             render_bean_tags(bean)
#             render_bean_body(bean)
#             render_bean_actions(user, bean, render_related_beans.refresh if show_related else None)
#             if show_related:
#                 render_related_beans(False)
#         ui.query('div.q-expansion-item__header').style(add=CONTENT_STYLE).classes(add="w-full")

#     return view


# def render_beans_as_list(beans: list[Bean], render_articles: bool, bean_render_func: Callable):
#     with ui.list().props(add="dense" if render_articles else "separator").classes("w-full") as view:        
#         for bean in beans:
#             with ui.item().classes(bean_item_class(render_articles)).style(bean_item_style):
#                 bean_render_func(bean)
#     return view

# def render_beans_as_carousel(beans: list[Bean], bean_render_func: Callable):
#     with ui.carousel(animated=True, arrows=True).props("swipeable vertical control-color=secondary").classes("h-full rounded-borders").style("background-color: #333333;") as view:          
#         for bean in beans:
#             with ui.carousel_slide(name=bean.url).classes("column no-wrap"):
#                 bean_render_func(bean)
#     return view




# def render_settings_as_text(settings: dict):
#     return ui.markdown("Currently showing news, blogs and social media posts on %s." % (", ".join([f"**{espressops.category_label(topic)}**" for topic in settings['topics']])))

# def render_separator():
#     return ui.separator().style("height: 5px;").classes("w-full m-0 p-0 gap-0")

# def render_banner_text(banner: str):
#     with ui.label(banner).classes("text-h5") as view:
#         ui.separator().style("margin-top: 5px;")
#     return view



# def _render_user_profile(settings: dict, user: dict):
#     user_connected = lambda source: bool(user and (source in user.get(espressops.CONNECTIONS, "")))

#     def update_connection(source, connect):        
#         if user and not connect:
#             # TODO: add a dialog
#             espressops.remove_connection(user, source)
#             del user[espressops.CONNECTIONS][source]
#         else:
#             ui.navigate.to(f"/{source}/login")
    
#     ui.link("u/"+user[K_ID], target=f"/channel/{user[K_ID]}").classes("text-bold")
#     with ui.row(wrap=False, align_items="stretch").classes("w-full gap-0"):  
#         with ui.column(align_items="stretch"):
#             # sequencing of bind_value and on_value_change is important.
#             # otherwise the value_change function will be called every time the page loads
#             ui.switch(text="Reddit", value=user_connected(REDDIT), on_change=lambda e: update_connection(REDDIT, e.sender.value)).tooltip("Link/Unlink Connection")
#             ui.switch(text="Slack", value=user_connected(SLACK), on_change=lambda e: update_connection(SLACK, e.sender.value)).tooltip("Link/Unlink Connection")
#         ui.space()
#         with ui.column(align_items="stretch").classes("gap-1"):
#             _render_user_image(user)
#             ui.button(text="Log out", icon="logout", color="negative", on_click=lambda: ui.navigate.to("/logout")).props("dense unelevated size=sm")
