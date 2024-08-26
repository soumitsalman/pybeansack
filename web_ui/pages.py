from shared import beanops, espressops, config, llmops, messages
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from icecream import ic
from .renderer import *
from .defaults import *
from shared import prompt_parser

parser = prompt_parser.InteractiveInputParser()

def render_home(settings):
    _render_shell(settings, "Home")

    tags = beanops.trending_tags(None, None, DEFAULT_WINDOW, DEFAULT_LIMIT)
    if tags:
        render_tags([tag.tags for tag in tags ])
    render_separator()   
    
    for section in TRENDING_TABS:
        ui.label(section['label']).classes('text-h5 w-full')
        beans = beanops.trending(None, None, None, section['kinds'], DEFAULT_WINDOW, None, MAX_ITEMS_PER_PAGE) 
        if beans:           
            render_beans_as_list(beans, True, lambda bean: render_expandable_bean(bean, True)).props("dense").classes("w-full")
        else:
            ui.label(messages.NOTHING_TRENDING)
        render_separator()

    render_settings_as_text(settings['search']) 
    ui.label(NAVIGATION_HELP).classes('text-caption')

def render_trending(settings, category: str, last_ndays: int):
    _render_shell(settings,"Trending")
    render_text_banner(category)

    tags = beanops.trending_tags(categories=category, kind=None, last_ndays=last_ndays, topn=DEFAULT_LIMIT)
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

def render_search(settings, query: str, keyword: str, kind, last_ndays: int):
    _render_shell(settings, "Search") 
    process_prompt = lambda: _trigger_search(settings, prompt_input.value)   
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).on('keydown.enter', process_prompt) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center') as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    
    kind = tuple(kind) if kind else None
    def _run_search():        
        if keyword or query:
            return (beanops.count_beans(query=query, categories=None, tags=keyword, kind=kind, last_ndays=last_ndays, topn=MAX_LIMIT),
                lambda start: beanops.search(query=query, categories=None, tags=keyword, kind=kind, last_ndays=last_ndays, start_index=start, topn=MAX_ITEMS_PER_PAGE))
        return (None, None)

    banner = query or keyword    
    if banner:
        # means there can be a search result
        render_text_banner(banner)
        count, beans_iter = _run_search()
        if count:
            render_beans_as_paginated_list(count, beans_iter)
        else:
            ui.label(messages.NOTHING_FOUND)

def _trigger_search(settings, prompt):   
    task, query, ctype, ndays, limit = parser.parse(prompt, settings['search'])
    if task in ["lookfor", "search"]:
        ui.navigate.to(make_navigation_target("/search", q=query, days=ndays))
    elif task == "trending":
        ui.navigate.to(make_navigation_target("/trending", category=query, days=ndays))
    else:
        ui.navigate.to(make_navigation_target("/search", q=prompt, days=ndays))

def _render_shell(settings, current_tab="Home"):
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
        _render_settings(settings)

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
            _render_login(settings)
            ui.button(on_click=settings_drawer.toggle, icon="settings").tooltip("Settings")

def _render_login(settings):
    user = logged_in_user(settings)    
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

def _render_settings(settings):   
    with ui.list().classes("w-full"):
        ui.item_label('Preferences').classes("text-subtitle1")
        with ui.item():
            with ui.item_section().bind_text_from(settings['search'], "last_ndays", lambda x: f"Last {x} days").classes("text-caption"):
                ui.slider(min=MIN_WINDOW, max=MAX_WINDOW, step=1, on_change=save_session_settings).bind_value(settings['search'], "last_ndays")
        with ui.item():
            ui.select(
                label="Topics of Interest", 
                options= espressops.get_system_topics(), 
                multiple=True,
                with_input=True,
                value=settings['search']['topics'],
                on_change=save_session_settings).bind_value(settings['search'], 'topics').props("use-chips filled").classes("w-full")
    
    ui.separator()

    ui.label('Connected Accounts').classes("text-subtitle1")
    ui.switch(text="Reddit", on_change=save_session_settings).bind_value(settings['connections'], config.REDDIT)
    ui.switch(text="Slack", on_change=save_session_settings).bind_value(settings['connections'], config.SLACK)
    
    # ui.space()
    # ui.button("Delete Account", color="negative", on_click=lambda: ui.notify("WRITE CODE")).props("flat").classes("self-right")

def render_user_registration(settings, user):
    _render_shell(settings, None)

    async def complete_registration():       
        ui.notify("Processing") 
        set_logged_in_user(settings, user)         
        espressops.register_user(user, settings['search']) 
        ui.navigate.to("/")

    def cancel_registration():
        if 'logged_in_user' in settings:
            del settings['logged_in_user'] 
        ui.navigate.to('/')

    async def import_topics():
        new_topics = llmops.analyze_reddit_posts(ic(user['name']))
        if new_topics:
            topics_panel.set_options(topics_panel.options + new_topics)
            topics_panel.set_value(new_topics)
        else:
            ui.notify(messages.NO_INTERESTS_MESSAGE)

    if user:
        render_text_banner("You Look New! Let's Get You Signed-up.")
        with ui.stepper().props("vertical").classes("w-full") as stepper:
            with ui.step("Sign Your Life Away") :
                ui.label("User Agreement").classes("text-h6").tooltip("Kindly read the documents and agree to the terms to reduce our chances of going to jail.")
                ui.link("What is Espresso", "https://github.com/soumitsalman/espresso/blob/main/README.md", new_tab=True)
                ui.link("Usage Terms & Policy", "https://github.com/soumitsalman/espresso/blob/main/documents/user-policy.md", new_tab=True)
                user_agreement = ui.checkbox(text="I have read and understood every single word in each of the links above.").tooltip("We are legally obligated to ask you this question.")
                with ui.stepper_navigation():
                    ui.button('Agreed', on_click=stepper.next).props("outline").bind_enabled_from(user_agreement, "value")
                    ui.button('Hell No!', color="negative", icon="cancel", on_click=cancel_registration).props("outline")
            with ui.step("Tell Me Your Dreams") :
                ui.label("Personalization").classes("text-h6")
                with ui.row(wrap=False, align_items="center").classes("w-full"):
                    ui.label("Name")
                    ui.input(user['name']).props("outlined").tooltip("We are saving this one").disable()
                
                ui.label("Your Interests")                                 
                topics_panel = ui.select(
                    label="Topics", with_input=True, multiple=True, 
                    options=espressops.get_system_topics()
                ).bind_value(settings['search'], 'topics').props("filled use-chips").classes("w-full").tooltip("We are saving this one too")

                # TODO: enable it later
                # if user['source'] == "reddit":
                #     ui.label("- or -").classes("text-caption self-center")
                #     ui.button("Analyze From Reddit", on_click=import_topics).classes("w-full")

                with ui.stepper_navigation():
                    ui.button("Done", icon="thumb_up", on_click=complete_registration).props("outline")
                    ui.button('Nope!', color="negative", icon="cancel", on_click=cancel_registration).props("outline")
    else:
        ui.label("You really thought we wouldn't have a check for this?!")

def render_login_failed(forward_path):
    with ui.dialog() as dialog, ui.card():
        ui.label("Welp! That didn't work").classes("self-center")
        with ui.row(align_items="stretch").classes("w-full center"):
            ui.button('Try Again', icon="login", on_click=lambda: [dialog.close(), ui.navigate.to(forward_path)])
            ui.button('Forget it', icon="cancel", color="negative", on_click=lambda: [dialog.close(), ui.navigate.to('/')])
    dialog.open()

def logged_in_user(settings: dict):
    return settings.get('logged_in_user')

def set_logged_in_user(settings, user):
    settings['logged_in_user'] = user    

def log_out_user(settings):
    if 'logged_in_user' in settings:
        del settings['logged_in_user']

def set_session_settings(settings, user):
    settings['search']['last_ndays'] = user[espressops.PREFERENCES]['last_ndays']
    settings['search']['topics'] = espressops.get_topics(user)
    for src in user[espressops.CONNECTIONS].keys():
        settings['connections'][src] = True

async def save_session_settings(settings):
    # ic(settings, "to store")
    pass

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