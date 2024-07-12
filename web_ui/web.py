from shared import beanops, userops, config
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from icecream import ic
from .render import *
from . import trending, search
   
def render_settings_panel(viewmodel):   
    with ui.list():
        ui.item_label('Search Settings').classes("text-subtitle1")
        with ui.item():
            with ui.item_section().bind_text_from(viewmodel['search'], "last_ndays", lambda x: f"Last {x} days"):
                ui.slider(min=1, max=30, step=1).bind_value(viewmodel['search'], "last_ndays")
        with ui.item():
            with ui.item_section().bind_text_from(viewmodel['search'], "topn", lambda x: f"Top {x} results"):
                ui.slider(min=1, max=50, step=1).bind_value(viewmodel['search'], "topn")
        with ui.item():
            with ui.expansion("Content Types", caption="Select content types to filter on"):
                ui.select(options=viewmodel['search']['content_types'], multiple=True).bind_value(viewmodel['search'], 'content_types').props("use-chips")
        with ui.item():
            with ui.expansion("Sources", caption="Select news and post sources to filter on"):
                ui.select(options=viewmodel['search']['sources'], with_input=True, multiple=True).bind_value(viewmodel['search'], 'sources').props("use-chips")
    
    ui.separator()

    with ui.column(align_items="stretch"):
        ui.label('Connections').classes("text-subtitle1")
        ui.switch(text="Slack")
        ui.switch(text="Reddit")
        ui.switch(text="LinkedIn")

def render_home_page(viewmodel: dict, settings: dict):
    ui.label("PLACEHOLDER")


def load(last_known_settings):
    ui.add_css(content="""
        @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;700&display=swap');
            
        body {
            font-family: 'Open Sans', sans-serif;
            color: #1A1A1A;        
        }
    """)

    usersettings = last_known_settings or _default_user_settings()
    pages = _create_pages()   

    #header
    with ui.header().classes(replace="row items-center"):
        with ui.avatar(square=True):
            ui.image("images/cafecito.png")

        with ui.tabs() as page_tabs:
            for p in pages['pages']:
                ui.tab(p['title'], icon=p['icon'])
        
        ui.space()
        ui.button(on_click=lambda: settings_drawer.toggle(), icon="settings").props('flat color=white').classes("self-right")

    async def load_page_viewmodel():
        page = next(p for p in pages['pages'] if p['title'] == pages[F_SELECTED])
        if page['title'] == 'Trending':
            trending.refresh_trending_viewmodel(page['viewmodel'], usersettings['search'])    

    #pages
    with ui.tab_panels(page_tabs, on_change=load_page_viewmodel).bind_value(pages, F_SELECTED).classes("w-full"):
        for p in pages['pages']:
            with ui.tab_panel(p['title']):
                p['render'](p['viewmodel'], usersettings['search'])


    # settings
    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        render_settings_panel(usersettings)

    return usersettings

def _create_pages():
    return {
        "pages": [
            {
                "title": "Home",
                "icon": "home",
                "render": render_home_page,
                "viewmodel": {}
            }, 
            {
                "title": "Search",
                "icon": "search",
                "render": search.render_search_page,
                "viewmodel": {
                    F_PROMPT: None,
                    F_PROCESSING_PROMPT: False,
                    F_RESPONSE_BANNER: None,
                    F_RESPONSE: None
                }
            }, 
            {
                "title": "Trending",
                "icon": "trending_up",
                "render": trending.render_trending_page,
                "viewmodel": {
                    F_CATEGORIES: {},
                    F_SELECTED: None
                }
            }
        ],
        F_SELECTED: "Home"
    }

def _default_user_settings():
    return {
        "search": {
            "last_ndays": config.DEFAULT_WINDOW,
            "topn": config.DEFAULT_LIMIT,
            "topics": userops.get_default_preferences(),
            "sources": beanops.get_sources(),
            "content_types": beanops.get_content_types()
        },
        "connections": {
            config.REDDIT: None,
            config.SLACK: None
        }            
    }


