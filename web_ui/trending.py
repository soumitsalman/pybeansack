from shared import beanops, userops
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from icecream import ic
from .items_render import *

async def _load_trending_nuggets(category, settings):    
    nuggets = beanops.highlights(category[F_NAME], settings['last_ndays'], settings['topn']) 
    category[F_NUGGETS] = [{'data': item} for item in nuggets]

async def render(settings: dict):  
    viewmodel = _create_page_viewmodel(settings)

    async def select_category():
        selected = viewmodel[F_SELECTED]
        if not viewmodel[F_CATEGORIES][selected][F_NUGGETS]:
            await _load_trending_nuggets(viewmodel[F_CATEGORIES][selected], settings)

    with ui.tabs(on_change=select_category, value=None).bind_value(viewmodel, F_SELECTED) as tabs:    
        for category in viewmodel[F_CATEGORIES].values():
            with ui.tab(category[F_NAME], label=category[F_NAME]):   
                ui.badge() \
                    .bind_text_from(category, F_NUGGETS, lambda x: str(len(x or []))) \
                    .bind_visibility_from(category, F_NUGGETS) \
                    .props("floating transparent")

    with ui.tab_panels(tabs):
        for category in viewmodel[F_CATEGORIES].values():
            with ui.tab_panel(category[F_NAME]) as panel:
                render_nuggets_as_expandable_list(category, settings).classes("w-full").style('flex: 1;') 

    for cat in viewmodel[F_CATEGORIES].values():
        await _load_trending_nuggets(cat, settings)

    return panel


def _create_page_viewmodel(settings):
    return {
        F_CATEGORIES: {cat: _create_category_viewmodel(cat) for cat in settings["topics"] or userops.get_default_preferences()},
        F_SELECTED: None
    }

        
def _create_category_viewmodel(cat: str):
    return {
        F_NAME:cat, 
        F_NUGGETS: None, 
        F_BEANS: None, 
        F_SELECTED: None,
        F_LISTVIEW: None
    }