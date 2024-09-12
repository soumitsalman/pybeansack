import asyncio
import threading
from connectors import redditor
from shared.config import *
from shared.messages import *
from shared import beanops, espressops, prompt_parser
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, background_tasks, run
from icecream import ic
from .renderer import *

SAVE_DELAY = 60
save_timer = threading.Timer(0, lambda: None)
save_lock = threading.Lock()

# shows everything that came in within the last 24 hours
def render_home(settings, user):    
    def _render():
        _render_beans_page(user, banner=None, urls=None, categories=settings['search']['topics'], last_ndays=MIN_WINDOW)
        render_separator()
        render_settings_as_text(settings['search']) 
        ui.label("Click on 📈 button for more trending stories by topics.\n\nClick on ⚙️ button to change topics.").classes('text-caption')

    render_shell(settings, user, "Home", _render)

def render_trending(settings, user, category: str, last_ndays: int):
    def _render():
        cat_label = espressops.category_label(category)
        if not cat_label:
            render_text(INVALID_INPUT)
            return
        _render_beans_page(user, banner=cat_label, urls=None, categories=category, last_ndays=last_ndays)
    render_shell(settings, user, "Trending", _render)

def render_channel(settings, user, channel_id: str, last_ndays: int):
    def _render():
        if not espressops.get_user({K_ID: channel_id}):
            render_text(CHANNEL_NOT_FOUND)
            return
        urls = espressops.channel_content(channel_id)
        if urls:
            _render_beans_page(user, banner=channel_id, urls=urls, categories=None, last_ndays=last_ndays)
        else:
            render_text(NOTHING_FOUND)
    render_shell(settings, user, "Trending", _render)

def _render_beans_page(user, banner: str, urls: list[str], categories: str|list[str], last_ndays: int):
    urls = tuple(urls) if isinstance(urls, list) else urls
    categories =  tuple(categories) if isinstance(categories, list) else categories
    if banner:
        render_text_banner(banner)

    background_tasks.create_lazy(_load_and_render_trending_tags(ui.element(), urls, categories, None, last_ndays, DEFAULT_LIMIT), name=f"trending-tags-{categories}")
    render_separator()

    with ui.tabs().props("dense").classes("w-full") as tab_headers:
        for tab in TRENDING_TABS:
            with ui.tab(name=tab['name'], label=""):
                with ui.row(wrap=False, align_items="stretch"):
                    ui.label(tab['label'])
                    count = beanops.count_beans(None, urls, categories, None, tab['kinds'], last_ndays, MAX_LIMIT+1)
                    if count:
                        ui.badge(rounded_number_with_max(count, MAX_LIMIT)).props("transparent")

    with ui.tab_panels(tabs=tab_headers, animated=True, value=TRENDING_TABS[0]['name']).props("swipeable").classes("w-full h-full m-0 p-0"):
        for tab in TRENDING_TABS:
            with ui.tab_panel(name=tab['name']).classes("w-full h-full m-0 p-0"):    
                background_tasks.create_lazy(
                    _load_and_render_trending_beans(render_skeleton_beans(3), urls, categories, tab['kinds'], last_ndays, user), 
                    name=f"trending-{tab['name']}"
                )    
                
async def _load_and_render_trending_tags(holder: ui.element, urls, categories, kinds, last_ndays, topn):
    tags = await run.io_bound(beanops.trending_tags, urls, categories, kinds, last_ndays, topn)    
    holder.clear()
    if tags:
        with holder:
            render_tags([tag.tags for tag in tags])

async def _load_and_render_trending_beans(holder: ui.element, urls, categories, kinds, last_ndays, for_user):     
    is_article = (NEWS in kinds) or (BLOG in kinds) 
    start_index = 0
    
    def get_beans():
        nonlocal start_index
        # retrieve 1 more than needed to check for whether to show the 'more' button 
        # this way I can check if there are more beans left in the pipe
        # because if there no more beans left no need to show the 'more' button
        beans = beanops.trending(urls, categories, kinds, last_ndays, start_index, MAX_ITEMS_PER_PAGE+1)
        start_index += MAX_ITEMS_PER_PAGE
        return beans[:MAX_ITEMS_PER_PAGE], (len(beans) > MAX_ITEMS_PER_PAGE)

    def render_beans(beans: list[Bean], panel: ui.list):
        with panel:        
            for bean in beans:                
                with ui.item().classes(bean_item_class(is_article)).style(bean_item_style):
                    render_expandable_bean(for_user, bean, True)

    async def next_page():
        nonlocal start_index, more_button
        with disable_button(more_button):
            beans, more = get_beans()
            render_beans(beans, beans_panel)         
        if not more:
            more_button.delete()
    
    beans, more = get_beans()
    holder.clear()
    with holder:   
        if not beans:
            ui.label(NOTHING_TRENDING_IN%last_ndays)
            return             
        beans_panel = ui.list().props("dense" if is_article else "separator").classes("w-full")       
        render_beans(beans, beans_panel)
        if more:
            more_button = ui.button("More Stories", on_click=next_page).props("unelevated icon-right=chevron_right")

def render_search(settings, user, query: str, keyword: str, kinds, last_ndays: int):
    def _render():
        process_prompt = lambda: _trigger_search(settings, prompt_input.value)   
        banner = query or keyword 
        with ui.input(placeholder=CONSOLE_PLACEHOLDER, autocomplete=CONSOLE_EXAMPLES).on('keydown.enter', process_prompt) \
            .props('rounded outlined input-class=mx-3').classes('w-full self-center') as prompt_input:
            ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense").tooltip("WE'LL BE RIGHT BACK")
        if banner: # means there can be a search result            
            render_text_banner(banner)            
            background_tasks.create_lazy(_search_and_render_beans(user, render_skeleton_beans(count=3), query, keyword, tuple(kinds) if kinds else None, last_ndays), name=f"search-{banner}")
    render_shell(settings, user, "Search", _render)

async def _search_and_render_beans(user: dict, holder: ui.element, query, keyword, kinds, last_ndays):         
    count = 0
    if query:            
        result = await run.io_bound(beanops.search, query=query, tags=keyword, kinds=kinds, last_ndays=last_ndays, start_index=0, topn=MAX_LIMIT)
        count, beans_iter = len(result), lambda start: result[start: start + MAX_ITEMS_PER_PAGE]
    elif keyword:
        count, beans_iter = beanops.count_beans(None, urls=None, categories=None, tags=keyword, kind=kinds, last_ndays=last_ndays, topn=MAX_LIMIT), \
            lambda start: beanops.search(query=None, tags=keyword, kinds=kinds, last_ndays=last_ndays, start_index=start, topn=MAX_ITEMS_PER_PAGE)

    holder.clear()
    with holder:
        if not count:
            ui.label(NOTHING_FOUND)
            return
        render_beans_as_paginated_list(beans_iter, count, lambda bean: render_expandable_bean(user, bean, False))

def _trigger_search(settings, prompt):   
    result = prompt_parser.console_parser.parse(prompt, settings['search'])
    if not result.task:
        ui.navigate.to(make_navigation_target("/search", q=result.query))
    if result.task in ["lookfor", "search"]:
        ui.navigate.to(make_navigation_target("/search", q=result.query, days=result.last_ndays))
    if result.task in ["trending"]:
        ui.navigate.to(make_navigation_target("/trending", q=result.query, days=result.last_ndays))
        
def render_shell(settings, user, current_tab: str, render_func: Callable):
    def render_topics_menu(topic):
        return (espressops.category_label(topic), lambda: ui.navigate.to(make_navigation_target(f"/trending/{topic}", days=settings['search']['last_ndays'])))

    def navigate(selected_tab):
        if selected_tab == "Home":
            ui.navigate.to("/")
        if selected_tab == "Search":
            ui.navigate.to("/search")

    # settings
    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        _render_settings(settings, user)

    # header
    with render_header(): 
        ui.label(APP_NAME).classes("text-bold app-name")
        ui.space()       
        with ui.tabs(on_change=lambda e: navigate(e.sender.value), value=current_tab):
            ui.tab(name="Home", label="", icon="home").tooltip("Home")           
            settings['search']['topics'] = sorted(settings['search']['topics'])
            with ui.tab(name="Trending", label="", icon='trending_up').tooltip("Trending News & Posts"):
                BindableNavigationMenu(render_topics_menu).bind_items_from(settings['search'], 'topics') 
            ui.tab(name="Search", label="", icon="search").tooltip("Search")          
        ui.button(icon = "settings", on_click=settings_drawer.toggle).props('flat stretch color='+("white" if user else "secondary")).tooltip("Settings")

    with ui.column(align_items="start").classes("responsive-container"):
        render_func()
        render_separator()
        with ui.row(align_items="center").classes("text-caption w-full").style("justify-content: center;"):
            ui.markdown("[[Terms & Conditions](https://github.com/soumitsalman/espresso/blob/main/README.md)]")
            ui.markdown("[[Security & Privacy Policy](https://github.com/soumitsalman/espresso/blob/main/README.md)]")
            ui.markdown("[[Project Cafecito](https://github.com/soumitsalman/espresso/blob/main/README.md)]")
            ui.markdown("[[About Us](https://github.com/soumitsalman/espresso/blob/main/documents/about-us.md)]")
        ui.label("Copyright © 2024 Project Cafecito. All rights reserved.").classes("text-caption w-full text-center")

def _render_login_status(settings: dict, user: dict):
    if user:
        with ui.row(wrap=False, align_items="start").classes("w-full"):            
            ui.link("/"+user[K_ID], target=f"/channel/{user[K_ID]}").classes("text-bold")
            with ui.column():
                _render_user_image(user)
                ui.button(text="Log out", icon="logout", color="negative", on_click=lambda: ui.navigate.to("/logout")).props("dense unelevated size=sm")
    else:
        with ui.label("Sign-up/Log-in").classes("text-subtitle1"):
            _render_login_buttons(settings, user)
    ui.separator()    

def _render_login_buttons(settings, user):
    with ui.row(wrap=False, align_items="center") as view:
        with ui.button(color="orange-10", on_click=lambda: ui.navigate.to("/reddit/login")).props("outline").tooltip("Continue with Reddit"):
            ui.avatar("img:https://www.redditinc.com/assets/images/site/Reddit_Icon_FullColor-1_2023-11-29-161416_munx.jpg", size="md", color="transparent")
        with ui.button(color="purple-10", on_click=lambda: ui.navigate.to('/slack/login')).props("outline").tooltip("Continue with Slack"):
            ui.avatar("img:https://a.slack-edge.com/80588/marketing/img/icons/icon_slack_hash_colored.png", square=True, size="md", color="transparent")
    return view

def _render_settings(settings: dict, user: dict):  
    async def delete_user():     
        # TODO: add a warning dialog   
        if user:
            espressops.unregister_user(user)
            ui.navigate.to("/logout")

    def update_connection(source, connect):        
        if user and not connect:
            # TODO: add a dialog
            espressops.remove_connection(user, source)
            del user[espressops.CONNECTIONS][source]
        else:
            ui.navigate.to(f"/{source}/login")

    async def save_session_settings():
        global save_lock, save_timer
        if user:
            with save_lock:
                if save_timer.is_alive():
                    save_timer.cancel()
                save_timer=threading.Timer(SAVE_DELAY, function=espressops.update_preferences, args=(user, settings['search']))
                save_timer.start()

    _render_login_status(settings, user)

    ui.label('Preferences').classes("text-subtitle1")
    # sequencing of bind_value and on_value_change is important.
    # otherwise the value_change function will be called every time the page loads
    with ui.label().bind_text_from(settings['search'], "last_ndays", lambda x: f"Last {x} days").classes("w-full"):
        ui.slider(min=MIN_WINDOW, max=MAX_WINDOW, step=1).bind_value(settings['search'], "last_ndays").on_value_change(save_session_settings)        
    ui.select(
        label="Topics of Interest", 
        options=_get_system_topic_options(), 
        multiple=True,
        with_input=True).bind_value(settings['search'], 'topics').on_value_change(save_session_settings).props("use-chips filled").classes("w-full")
    
    ui.separator()

    ui.label('Accounts').classes("text-subtitle1")
    user_connected = lambda source: bool(user and (source in user.get(espressops.CONNECTIONS, "")))
    reddit_connect = ui.switch(text="Reddit", value=user_connected(REDDIT)).on_value_change(lambda: update_connection(REDDIT, reddit_connect.value)).tooltip("Link/Unlink Connection")
    slack_connect = ui.switch(text="Slack", value=user_connected(SLACK)).on_value_change(lambda: update_connection(SLACK, slack_connect.value)).tooltip("Link/Unlink Connection")

    if user:
        ui.space()
        ui.button("Delete Account", color="negative", on_click=delete_user).props("flat").classes("self-right").tooltip("Deletes your account, all connections and preferences")

def render_user_registration(settings: dict, temp_user: dict, success_func: Callable, failure_func: Callable):
    with render_header():
        ui.label(APP_NAME).classes("text-bold app-name")

    if not temp_user:
        ui.label("You really thought I wouldn't check for this?!")
        ui.button("My Bad!", on_click=failure_func)
        return
    
    def extract_topics():
        text = redditor.collect_user_as_text(temp_user['name'], limit=10)     
        if len(text) >= 100:                       
            return espressops.match_categories(text)

    async def trigger_reddit_import(sender: ui.element):
        with disable_button(sender):    
            sender.props(":loading=true")  
            new_topics = await run.io_bound(extract_topics)
            if not new_topics:
                ui.notify(NO_INTERESTS_MESSAGE)
                return            
            settings['search']['topics'] = new_topics
            sender.props(":loading=false")
    
    render_text_banner("You Look New! Let's Get You Signed-up.")
    with ui.stepper().props("vertical").classes("w-full") as stepper:
        with ui.step("Sign Your Life Away") :
            ui.label("User Agreement").classes("text-h6").tooltip("Kindly read the documents and agree to the terms to reduce our chances of going to jail.")
            ui.link("What is Espresso", "https://github.com/soumitsalman/espresso/blob/main/README.md", new_tab=True)
            ui.link("Usage Terms & Policy", "https://github.com/soumitsalman/espresso/blob/main/documents/user-policy.md", new_tab=True)
            user_agreement = ui.checkbox(text="I have read and understood every single word in each of the links above.").tooltip("We are legally obligated to ask you this question.")
            with ui.stepper_navigation():
                ui.button('Agreed', on_click=stepper.next).props("outline").bind_enabled_from(user_agreement, "value")
                ui.button('Hell No!', color="negative", icon="cancel", on_click=failure_func).props("outline")
        with ui.step("Tell Me Your Dreams") :
            ui.label("Personalization").classes("text-h6")
            with ui.row(wrap=False, align_items="center").classes("w-full"):
                ui.label("User ID")
                temp_user[K_ID] = espressops.convert_new_userid(f"{temp_user['name']}@{temp_user[K_SOURCE]}")
                ui.input().bind_value(temp_user, K_ID).props("outlined").tooltip("We are saving this one") 
                _render_user_image(temp_user)        
            ui.label("Your Interests")      
            if temp_user['source'] == "reddit":     
                ui.button("Analyze From Reddit", on_click=lambda e: trigger_reddit_import(e.sender)).classes("w-full")          
                ui.label("- or -").classes("text-caption self-center")                         
            ui.select(
                label="Topics", with_input=True, multiple=True, 
                options=_get_system_topic_options()
            ).bind_value(settings['search'], 'topics').props("filled use-chips").classes("w-full").tooltip("We are saving this one too")

            with ui.stepper_navigation():
                ui.button("Done", icon="thumb_up", on_click=lambda: success_func(espressops.register_user(temp_user, settings['search']))).props("outline")
                ui.button('Nope!', color="negative", icon="cancel", on_click=failure_func).props("outline")

def _render_user_image(user):  
    if user and user.get(espressops.IMAGE_URL):
        ui.image(user.get(espressops.IMAGE_URL)).classes("w-28 h-28")

def render_login_failed(success_forward, failure_forward):
    with render_header():
        ui.label(APP_NAME).classes("text-bold app-name")

    ui.label("Welp! That didn't work").classes("self-center")
    with ui.row(align_items="stretch").classes("w-full").style("justify-content: center;"):
        ui.button('Try Again', icon="login", on_click=lambda: ui.navigate.to(success_forward))
        ui.button('Forget it', icon="cancel", color="negative", on_click=lambda: ui.navigate.to(failure_forward))

def _get_system_topic_options():    
    return {topic[K_ID]: topic[K_TEXT] for topic in espressops.get_system_categories()}

def _get_user_topic_values(registered_user):
    return [topic[K_ID] for topic in espressops.get_categories(registered_user)]