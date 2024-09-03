import threading
from connectors import redditor
from shared import beanops, espressops, config, llmops, messages
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, background_tasks
from icecream import ic
from .renderer import *
from .defaults import *
from shared import prompt_parser

parser = prompt_parser.InteractiveInputParser()
save_timer = threading.Timer(0, lambda: None)
save_lock = threading.Lock()

def render_home(settings, user):
    render_shell(settings, user, "Home")
    
    # pull in everything that came in within the last 24 hours
    categories = tuple(settings['search']['topics']) if user else None
    tags = beanops.trending_tags(categories, None, MIN_WINDOW, DEFAULT_LIMIT)
    if tags:
        render_tags([tag.tags for tag in tags ])
        render_separator()
    
    for section in TRENDING_TABS:
        ui.label(section['label']).classes('text-h5 w-full')
        beans = beanops.trending(None, categories, None, section['kinds'], MIN_WINDOW, 0, DEFAULT_LIMIT) 
        if beans:           
            render_beans_as_list(beans, NEWS in section['kinds'], lambda bean: render_expandable_bean(bean, True)).props("dense" if NEWS in section['kinds'] else "separator").classes("w-full")
        else:
            ui.label(messages.NOTHING_TRENDING)
        render_separator()

    render_settings_as_text(settings['search']) 
    ui.label(NAVIGATION_HELP).classes('text-caption')
    render_separator()

    with ui.row(align_items="center").classes("text-caption").style("self-align: center; text-align: center;"):
        ui.markdown("[[Project Cafecito](https://github.com/soumitsalman/espresso/blob/main/README.md)]")
        ui.markdown("[[Espresso](https://github.com/soumitsalman/espresso/blob/main/README.md)]")
        ui.markdown("[[About Us](https://github.com/soumitsalman/espresso/blob/main/documents/about-us.md)]")

def render_trending(settings, user, category: str, last_ndays: int):
    render_shell(settings, user, "Trending")
    render_text_banner(category)

    tags = beanops.trending_tags(categories=category, kinds=None, last_ndays=last_ndays, topn=DEFAULT_LIMIT)
    if tags:
        render_tags([tag.tags for tag in tags])
        render_separator()

    with ui.tabs().props("dense").classes("w-full") as tab_headers:
        for tab in TRENDING_TABS:
            ui.tab(name=tab['name'], label=tab['label'])

    with ui.tab_panels(tabs=tab_headers, animated=True, value=TRENDING_TABS[0]['name']).props("swipeable").classes("w-full h-full m-0 p-0"):
        for tab in TRENDING_TABS:
            with ui.tab_panel(name=tab['name']).classes("w-full h-full m-0 p-0"):        
                total = beanops.count_beans(None, category, None, tab["kinds"], last_ndays, MAX_LIMIT)        
                if total:                     
                    _render_beans_page(category, tab["kinds"], last_ndays, total)
                else:
                    ui.label(messages.NOTHING_TRENDING_IN%last_ndays)

def _render_beans_page(category, kinds, last_ndays, total):
    is_article = (NEWS in kinds) or (BLOG in kinds)       
    start_index = 0 

    def load_page():
        nonlocal start_index, panel, more
        beans = beanops.trending(None, category, None, kinds, last_ndays, start_index, MAX_ITEMS_PER_PAGE)
        start_index += MAX_ITEMS_PER_PAGE

        with panel:        
            for bean in beans:                
                with ui.item().classes(bean_item_class(is_article)).style(bean_item_style):
                    render_expandable_bean(bean, True)
        if start_index >= total:
            more.set_visibility(False)

    panel = ui.list().props("dense" if is_article else "separator").classes("w-full")
    more = ui.button("More Stories", on_click=load_page).props("unelevated icon-right=chevron_right")
    load_page()

def render_search(settings, user, query: str, keyword: str, kinds, last_ndays: int):
    render_shell(settings, user, "Search") 

    process_prompt = lambda: _trigger_search(settings, prompt_input.value)   
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).on('keydown.enter', process_prompt) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center') as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    
    banner = query or keyword    
    if banner:
        # means there can be a search result
        render_text_banner(banner)
        holder = ui.element()
        background_tasks.create(_search_and_render(holder, query, keyword, tuple(kinds) if kinds else None, last_ndays), name=f"search-{banner}")

async def _search_and_render(holder, query, keyword, kinds, last_ndays):         
    count = 0
    if query:            
        result = beanops.search(query=query, categories=None, tags=keyword, kinds=kinds, last_ndays=last_ndays, start_index=0, topn=MAX_LIMIT)
        count, beans_iter = len(result), lambda start: result[start: start + MAX_ITEMS_PER_PAGE]
    elif keyword:
        count, beans_iter = beanops.count_beans(query=None, categories=None, tags=keyword, kind=kinds, last_ndays=last_ndays, topn=MAX_LIMIT), \
            lambda start: beanops.search(query=None, categories=None, tags=keyword, kinds=kinds, last_ndays=last_ndays, start_index=start, topn=MAX_ITEMS_PER_PAGE)

    with holder:
        if not count:
            ui.label(messages.NOTHING_FOUND)
            return
        render_beans_as_paginated_list(count, beans_iter)

async def _trigger_search(settings, prompt):   
    task, query, ctype, ndays, limit = parser.parse(prompt, settings['search'])
    if task in ["lookfor", "search"]:
        ui.navigate.to(make_navigation_target("/search", q=query, days=ndays))
    elif task == "trending":
        ui.navigate.to(make_navigation_target("/trending", category=query, days=ndays))
    else:
        ui.navigate.to(make_navigation_target("/search", q=prompt, days=ndays))

def render_shell(settings, user, current_tab="Home"):
    # set themes  
    ui.colors(secondary=SECONDARY_COLOR)
    ui.add_css(content=CSS)
    
    def render_topics_menu(topic):
        return (topic, 
                make_navigation_target("/trending", category=topic, days=settings['search']['last_ndays']),
                beanops.count_beans(query=None, categories=topic, tags=None, kind=None, last_ndays=settings['search']['last_ndays'], topn=MAX_LIMIT))

    def navigate(selected_tab):
        if selected_tab == "Home":
            ui.navigate.to("/")
        elif selected_tab == "Search":
            ui.navigate.to("/search")

    # settings
    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        _render_settings(settings, user)

    # header
    with ui.header().classes(replace="row"):
        with ui.avatar(square=True).tooltip("Espresso by Cafecit.io"):
            ui.image("images/cafecito.png")
        
        with ui.tabs(on_change=lambda: navigate(tab_selector.value), value=current_tab) as tab_selector:
            ui.tab(name="Home", label="", icon="home").tooltip("Home")           
            settings['search']['topics'] = sorted(settings['search']['topics'])
            with ui.tab(name="Trending", label="", icon='trending_up').tooltip("Trending News & Posts"):
                BindableNavigationMenu(render_topics_menu).bind_items_from(settings['search'], 'topics') 
            ui.tab(name="Search", label="", icon="search").tooltip("Search")

        ui.space()

        with ui.button_group().props('flat color=white').classes("self-right"):
            _render_login(settings, user)
            ui.button(on_click=settings_drawer.toggle, icon="settings").tooltip("Settings")

def _render_login(settings, user): 
    if user:
        return ui.button(icon='logout', on_click=lambda: ui.navigate.to("/logout")).tooltip("Log-out")
    else:
        with ui.button(icon='login').tooltip("Log-in / Sign-up") as view:
            with ui.menu():
                with ui.menu_item(text="Continue with Reddit", on_click=lambda: ui.navigate.to("/reddit/login")).style("border-radius: 20px; color: #FF4500").classes("border-[1px] text-bold m-1"):
                    ui.avatar("img:https://www.redditinc.com/assets/images/site/Reddit_Icon_FullColor-1_2023-11-29-161416_munx.jpg", color="transparent")
                with ui.menu_item(text="Continue with Slack", on_click=lambda: ui.navigate.to('/slack/login')).style("border-radius: 20px; color: #4A154B").classes("border-[1px] text-bold m-1"):
                    ui.avatar("img:https://a.slack-edge.com/80588/marketing/img/icons/icon_slack_hash_colored.png", square=True, color="transparent")
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

    ui.label('Preferences').classes("text-subtitle1")
    # sequencing of bind_value and on_value_change is important.
    # otherwise the value_change function will be called every time the page loads
    with ui.label().bind_text_from(settings['search'], "last_ndays", lambda x: f"Last {x} days").classes("w-full"):
        ui.slider(min=MIN_WINDOW, max=MAX_WINDOW, step=1).bind_value(settings['search'], "last_ndays").on_value_change(save_session_settings)        
    ui.select(
        label="Topics of Interest", 
        # TODO: merge it with user preferences
        options=espressops.get_system_topics(), 
        multiple=True,
        with_input=True).bind_value(settings['search'], 'topics').on_value_change(save_session_settings).props("use-chips filled").classes("w-full")
    
    ui.separator()

    ui.label('Accounts').classes("text-subtitle1")
    user_connected = lambda source: bool(user and (source in user.get(espressops.CONNECTIONS, "")))
    reddit_connect = ui.switch(text="Reddit", value=user_connected(config.REDDIT)).on_value_change(lambda: update_connection(config.REDDIT, reddit_connect.value)).tooltip("Link/Unlink Connection")
    slack_connect = ui.switch(text="Slack", value=user_connected(config.SLACK)).on_value_change(lambda: update_connection(config.SLACK, slack_connect.value)).tooltip("Link/Unlink Connection")

    if user:
        ui.space()
        ui.button("Delete Account", color="negative", on_click=delete_user).props("flat").classes("self-right").tooltip("Deletes your account, all connections and preferences")




def render_user_registration(settings, temp_user, success_func: Callable, failure_func: Callable):
    # with ui.card():
    if not temp_user:
        ui.label("You really thought I wouldn't check for this?!")
        ui.button("My Bad!", on_click=failure_func)
        return
    
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
                ui.label("Name")
                ui.input(temp_user['name']).props("outlined").tooltip("We are saving this one").disable()
            
            ui.label("Your Interests")                                 
            topics_panel = ui.select(
                label="Topics", with_input=True, multiple=True, 
                options=espressops.get_system_topics()
            ).bind_value(settings['search'], 'topics').props("filled use-chips").classes("w-full").tooltip("We are saving this one too")

            # TODO: enable it later
            if temp_user['source'] == "reddit":
                ui.label("- or -").classes("text-caption self-center")
                ui.button("Analyze From Reddit", on_click=lambda e: _trigger_reddit_import(temp_user['name'], topics_panel, e.sender)).classes("w-full")

            with ui.stepper_navigation():
                ui.button("Done", icon="thumb_up", on_click=lambda: success_func(espressops.register_user(temp_user, settings['search']))).props("outline")
                ui.button('Nope!', color="negative", icon="cancel", on_click=failure_func).props("outline")
  
def _trigger_reddit_import(username, selector: ui.select, button: ui.button):
    with disable_button(button):
        text = redditor.collect_user_as_text(username, limit=10).strip()
        new_topics = espressops.search_categories(text) if len(text) > 100 else None
        if new_topics:
            selector.set_options(selector.options + new_topics)
            selector.set_value(new_topics)
        else:
            ui.notify(messages.NO_INTERESTS_MESSAGE)

def render_login_failed(success_forward, failure_forward):
    with ui.card():
        ui.label("Welp! That didn't work").classes("self-center")
        with ui.row(align_items="stretch").classes("w-full center"):
            ui.button('Try Again', icon="login", on_click=lambda: ui.navigate.to(success_forward))
            ui.button('Forget it', icon="cancel", color="negative", on_click=lambda: ui.navigate.to(failure_forward))

def create_default_settings():
    return {
        "search": {
            "last_ndays": DEFAULT_WINDOW,            
            "topics": config.DEFAULT_CATEGORIES
        },
        "connections": {
            config.REDDIT: False,
            config.SLACK: False
        }            
    }

TRENDING_TABS = [
        {
            "name": "articles", 
            "label": "📰 News & Articles",
            "kinds": (NEWS, BLOG)
        },
        {
            "name": "posts", 
            "label": "🗣️ Social Media",
            "kinds": (POST, COMMENT)
        }
    ]