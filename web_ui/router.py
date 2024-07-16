import json
from shared import beanops, userops, config
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui
from icecream import ic
from . import trending, search, render, theme
   
HOME = 'home'
CONSOLE = 'console'
TRENDING = 'trending'
SOCIALMEDIA = 'social_media'
URL_SEARCH = 'url_search'

def render_home(viewmodel):
    ui.markdown(render.settings_markdown(viewmodel['settings']['search']))

def render_settings_panel(usersettings):   
    with ui.list():
        ui.item_label('Search Settings').classes("text-subtitle1")
        with ui.item():
            with ui.item_section().bind_text_from(usersettings['search'], "last_ndays", lambda x: f"Last {x} days"):
                ui.slider(min=1, max=30, step=1).bind_value(usersettings['search'], "last_ndays")
        with ui.item():
            with ui.item_section().bind_text_from(usersettings['search'], "topn", lambda x: f"Top {x} results"):
                ui.slider(min=1, max=50, step=1).bind_value(usersettings['search'], "topn")
        with ui.item():
            with ui.expansion("Topics of Interest", caption="Select topics your are interesting in"):
                ui.select(options=userops.get_default_preferences(), multiple=True).bind_value(usersettings['search'], 'topics').props("use-chips")
        with ui.item():
            with ui.expansion("Content Types", caption="Select content types to filter on"):
                ui.select(options=usersettings['search']['content_types'], multiple=True).bind_value(usersettings['search'], 'content_types').props("use-chips")
        with ui.item():
            with ui.expansion("Sources", caption="Select news and post sources to filter on"):
                ui.select(options=usersettings['search']['sources'], with_input=True, multiple=True).bind_value(usersettings['search'], 'sources').props("use-chips")
    
    ui.separator()

    with ui.column(align_items="stretch"):
        ui.label('Connections').classes("text-subtitle1")
        ui.switch(text="Slack")
        ui.switch(text="Reddit")
        ui.switch(text="LinkedIn")

def load_page(page, viewmodel, *args):    
    ui.colors(secondary=theme.SECONDARY_COLOR)
    ui.add_css(content=theme.CSS)

    render.tag_route = lambda kind, keyword: navigate_to(kind, viewmodel, keyword)

    # header
    with ui.header().classes(replace="row items-center"):
        with ui.avatar(square=True):
            ui.image("images/cafecito.png")
        
        with ui.tabs() as page_tabs:
            [ui.tab(name=p['title'], icon=p['icon']).tooltip(p['title']) for p in PAGES]
        ui.space()
        ui.button(on_click=lambda: settings_drawer.toggle(), icon="settings").props('flat color=white').classes("self-right")

    # settings
    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        render_settings_panel(viewmodel['settings']) 

    async def select_page():
        if viewmodel[render.F_SELECTED]:
            await trending.load_nuggets(viewmodel)

    # panels
    with ui.tab_panels(tabs=page_tabs, on_change=select_page).bind_value(viewmodel, render.F_SELECTED):
        for p in PAGES:
            with ui.tab_panel(p['title']):
                p["render"](viewmodel)

    # now load the page that was asked to load
    navigate_to(page, viewmodel, *args)

def navigate_to(page, viewmodel, *args):
    if any(p for p in PAGES if p['title']==page):
        viewmodel[render.F_SELECTED] = page
    elif page == "SearchBeans":
        search.load_beans_by_keyword(viewmodel, *args)
        viewmodel[render.F_SELECTED] = "Search"
    elif page == "SearchNuggets":
        search.load_nuggets_by_keyword(viewmodel, *args)
        viewmodel[render.F_SELECTED] = "Search"

def create_default_settings():
    return {
        "search": {
            "last_ndays": config.DEFAULT_WINDOW,
            "topn": config.DEFAULT_LIMIT,
            "topics": [],
            "sources": beanops.get_sources(),
            "content_types": beanops.get_content_types()
        },
        "connections": {
            config.REDDIT: None,
            config.SLACK: None
        }            
    }

PAGES = [
    {
        "title": "Home",
        "icon": "home",
        "target": "/",
        "render": render_home
    }, 
    {
        "title": "Search",
        "icon": "search",
        "target": "/search",
        "render": search.render
    }, 
    {
        "title": "Trending",
        "icon": "trending_up",
        "target": "/trending",
        "render": trending.render        
    }
]




