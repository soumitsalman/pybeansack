import json
from shared import beanops, userops, config
from pybeansack.datamodels import *
from . import items_render
from web_ui.custom_ui import *
from nicegui import ui
from icecream import ic
from . import trending, search
   
def render_home(usersettings):
    ui.markdown(items_render.settings_markdown(usersettings['search']))

def render_trending(usersettings):
    trending.render(usersettings['search'])

def render_search(usersettings):
    search.render(usersettings['search'])

def render_beans(usersettings):
    ui.markdown(items_render.settings_markdown(usersettings['search']))

def render_nuggets(usersettings):
    ui.markdown(items_render.settings_markdown(usersettings['search']))

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

def load_page(page, usersettings):
    ui.add_css(content=_CSS)

    #header
    with ui.header().classes(replace="row items-center"):
        with ui.avatar(square=True):
            ui.image("images/cafecito.png")

        with ui.button_group().props("flat dense"):
            [ui.button(text=p['title'], icon=p['icon'], on_click=lambda p=p: ui.navigate.to(p['target'])) for p in _PAGES]
        
        ui.space()
        ui.button(on_click=lambda: settings_drawer.toggle(), icon="settings").props('flat color=white').classes("self-right")

    # settings
    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        render_settings_panel(usersettings)

    page(usersettings)

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


_PAGES = [
    {
        "title": "Home",
        "icon": "home",
        "target": "/",
        "render": render_home,
        "viewmodel": {}
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

_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;700&display=swap');
    
body {
    font-family: 'Open Sans', sans-serif;
    color: #1A1A1A;        
}
"""



