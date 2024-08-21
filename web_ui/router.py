from shared import beanops, espressops, config, messages
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from icecream import ic
from .renderer import *
from .defaults import *
from shared import prompt_parser

parser = prompt_parser.InteractiveInputParser()

async def render_home(settings):
    render_shell(settings, "Home")

    tags = beanops.trending_tags(None, None, DEFAULT_WINDOW, DEFAULT_LIMIT)
    if tags:
        render_tags([tag.tags for tag in tags ])
    render_separator()   
    
    ui.label("📰 News").classes('text-h5 w-full')
    news = beanops.trending(None, None, None, (NEWS), DEFAULT_WINDOW, None, MAX_ITEMS_PER_PAGE) 
    if news:           
        render_beans_as_list(news, True, render_expandable_bean).props("dense").classes("w-full")
    else:
        ui.label(messages.NOTHING_TRENDING)
    render_separator()

    ui.label("🗣️ Social Media").classes('text-h5 w-full')
    posts = beanops.trending(None, None, None, (POST), DEFAULT_WINDOW, None, MAX_ITEMS_PER_PAGE)    
    if posts:
        render_beans_as_list(posts, False, render_expandable_bean).props("separator").classes("w-full")
    else:
        ui.label(messages.NOTHING_TRENDING)
    render_separator()

    render_settings_as_text(settings['search']) 
    ui.label(NAVIGATION_HELP).classes('text-caption')

async def render_trending_news(settings: dict, category: str, last_ndays: int):  
    render_shell(settings,"Trending News")
    render_text_banner(f"📰 {category}")
    
    kinds = (NEWS, BLOG)
    total = beanops.count_beans(None, category, None, kinds, last_ndays, MAX_LIMIT)        
    if total: 
        tags = beanops.trending_tags(categories=category, kind=kinds, last_ndays=last_ndays, topn=DEFAULT_LIMIT)
        render_tags([tag.tags for tag in tags])
        render_separator()
        await _render_beans_page(category, kinds, last_ndays, total)
    else:
        ui.label(messages.NOTHING_TRENDING_IN%last_ndays)

async def render_hot_posts(settings: dict, category: str, last_ndays: int):  
    render_shell(settings, "Hot Posts")
    render_text_banner(f"🗣️ {category}")
    
    kinds = (POST, COMMENT)
    total = beanops.count_beans(None, category, None, kinds, last_ndays, MAX_LIMIT)     
    if total: 
        tags = beanops.trending_tags(categories=category, kind=kinds, last_ndays=last_ndays, topn=DEFAULT_LIMIT)     
        render_tags([tag.tags for tag in tags])
        render_separator()         
        await _render_beans_page(category, kinds, last_ndays, total)
    else:
        ui.label(messages.NOTHING_HOT_IN%last_ndays)

async def _render_beans_page(category, kinds, last_ndays, total):
    is_article = (NEWS in kinds) or (BLOG in kinds)
    item_class = "w-full border-[1px]" if is_article else "w-full"
    item_style = "border-radius: 5px; margin-bottom: 5px;" if is_article else "margin-bottom: 5px;"    
    start_index = 0 

    async def load_page():
        nonlocal start_index, panel, more
        beans = beanops.trending(None, category, None, kinds, last_ndays, start_index, MAX_ITEMS_PER_PAGE)
        start_index += MAX_ITEMS_PER_PAGE

        with panel:        
            for bean in beans:                
                with ui.item().classes(item_class).style(item_style):
                    render_expandable_bean(bean)
        if start_index >= total:
            more.set_visibility(False)

    panel = ui.list().props("dense" if is_article else "separator").classes("w-full")
    more = ui.button("More Stories", on_click=load_page).props("unelevated icon-right=chevron_right")
    await load_page()

async def render_search(settings, query: str, keyword: str, kind, last_ndays: int):
    render_shell(settings, "Search") 
    process_prompt = lambda: _trigger_search(settings, prompt_input.value)   
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).on('keydown.enter', process_prompt) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center') as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    
    kind = tuple(kind) if kind else None
    async def _run_search():        
        if keyword or query:
            return (beanops.count_beans(query=query, categories=None, tags=keyword, kind=kind, last_ndays=last_ndays, topn=MAX_LIMIT),
                lambda start: beanops.search(query=query, categories=None, tags=keyword, kind=kind, last_ndays=last_ndays, start_index=start, topn=MAX_ITEMS_PER_PAGE))
        return (None, None)

    banner = query or keyword    
    if banner:
        # means there can be a search result
        render_text_banner(banner)
        count, beans_iter = await _run_search()
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

def render_shell(settings, current_tab="Home"):
    # set themes  
    ui.colors(secondary=SECONDARY_COLOR)
    ui.add_css(content=CSS)
    
    def render_news_topic(topic):
        return (topic, 
                make_navigation_target("/trending", category=topic, days=settings['search']['last_ndays']),
                beanops.count_beans(query=None, categories=topic, tags=None, kind=(NEWS, BLOG), last_ndays=settings['search']['last_ndays'], topn=MAX_LIMIT))

    def render_post_topic(topic):
        return (topic, 
                make_navigation_target("/hot", category=topic, days=settings['search']['last_ndays']),
                beanops.count_beans(query=None, categories=topic, tags=None, kind=(POST, COMMENT), last_ndays=settings['search']['last_ndays'], topn=MAX_LIMIT))

    def navigate(selected_tab):
        if selected_tab == "Home":
            ui.navigate.to("/")
        elif selected_tab == "Search":
            ui.navigate.to("/search")

    # login = _render_login(settings)

    # header
    with ui.header().classes(replace="row"):
        with ui.avatar(square=True):
            ui.image("images/cafecito.png")
        
        with ui.tabs(on_change=lambda: navigate(tab_selector.value), value=current_tab) as tab_selector:
            ui.tab(name="Home", label="", icon="home").tooltip("Home")           
            settings['search']['topics'] = sorted(settings['search']['topics'])
            with ui.tab(name="Trending News", label="", icon='trending_up').tooltip("Trending News"):
                BindableNavigationMenu(render_news_topic).bind_items_from(settings['search'], 'topics')            
            with ui.tab(name="Hot Posts", label="", icon="local_fire_department").tooltip("Hot Posts"):
                BindableNavigationMenu(render_post_topic).bind_items_from(settings['search'], 'topics')
            ui.tab(name="Search", label="", icon="search").tooltip("Search")

        ui.space()
        with ui.button_group().props('flat color=white').classes("self-right"):
            # ui.button(on_click=login.open, icon="login").tooltip("Login")
            ui.button(on_click=lambda: settings_drawer.toggle(), icon="settings").tooltip("Settings")

    # settings
    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        _render_settings(settings)

def _render_login(settings):
    with ui.dialog() as view, ui.card():
        ui.button("Continue with Reddit", on_click=lambda: view.submit("Reddit")).props("rounded")
        ui.button("Continue with Slack", on_click=lambda: view.submit("Slack")).props("rounded")
        # TODO: do a close somewhere here
    return view

def _render_settings(settings):   
    with ui.list().classes("w-full"):
        ui.item_label('Preferences').classes("text-subtitle1")
        with ui.item():
            with ui.item_section().bind_text_from(settings['search'], "last_ndays", lambda x: f"Last {x} days").classes("text-caption"):
                ui.slider(min=MIN_WINDOW, max=MAX_WINDOW, step=1).bind_value(settings['search'], "last_ndays")
        with ui.item():
            # with ui.expansion("Topics of Interest", caption="Select topics your are interesting in"):
            ui.select(
                label="Topics of Interest", 
                options= espressops.get_topics(espressops.SYSTEM), 
                multiple=True,
                with_input=True,
                value=settings['search']['topics']).bind_value(settings['search'], 'topics').props("use-chips filled").classes("w-full")
    
    ui.separator()

    with ui.column(align_items="stretch"):
        ui.label('Connected Accounts').classes("text-subtitle1")
        ui.switch(text="Slack", on_change=lambda: ui.notify(messages.NO_ACTION))
        ui.switch(text="Reddit", on_change=lambda: ui.notify(messages.NO_ACTION))


def create_default_settings():
    return {
        "search": {
            "last_ndays": DEFAULT_WINDOW,            
            "topics": config.DEFAULT_CATEGORIES
        },
        "connections": {
            config.REDDIT: None,
            config.SLACK: None
        }            
    }
