from shared import beanops, userops
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from icecream import ic
from .render import *

def load_trending_nuggets_for_category(category, settings):    
    nuggets = beanops.highlights(category[F_NAME], settings['last_ndays'], settings['topn']) 
    category[F_NUGGETS] = [{'data': item} for item in nuggets]

def render_nuggets_as_expandable_list(viewmodel: dict, settings: dict):
    async def load_beans(nugget):
        if nugget[F_SELECTED]:
            nugget[F_BEANS] = await run.io_bound(beanops.get_beans_by_nugget, nugget['data'].id, tuple(settings['content_types']), settings['last_ndays'], settings['topn'])

    def render_nugget_as_expandable_item(nugget: dict):
        bean_count = beanops.count_beans_for_nugget(nugget['data'].id, tuple(settings['content_types']), settings['last_ndays'], settings['topn'])   
        with ui.item() as view:
            with ui.column(align_items="start"):                        
                render_nugget_banner(nugget['data'])  
                with ui.expansion(
                        group="group", 
                        text=nugget['data'].description, 
                        caption=f"{counter_text(bean_count)} items",
                        on_value_change=lambda nugget=nugget: load_beans(nugget), 
                        value=False
                    ).bind_value(nugget, F_SELECTED):
                    render_beans_as_bindable_list(nugget, F_BEANS)
                ui.separator()
        return view

    # ui.label(messages.NOTHING_TRENDING).bind_visibility_from(viewmodel, F_NUGGETS, lambda x: not x)
    return BindableList(render_nugget_as_expandable_item).bind_items_from(viewmodel, F_NUGGETS)

def render(viewmodel: dict):  
    viewmodel = _init_page_viewmodel(viewmodel)
    trendingmodel = viewmodel['trending']
    settings = viewmodel['settings']['search']  

    async def select_category():
        selected = trendingmodel[F_SELECTED]
        if selected:
            await run.io_bound(load_trending_nuggets_for_category, trendingmodel[F_CATEGORIES][selected], settings)

    with ui.tabs(on_change=select_category, value=None).bind_value(trendingmodel, F_SELECTED) as tabs:    
        for category in trendingmodel[F_CATEGORIES].values():
            with ui.tab(category[F_NAME], label=category[F_NAME]):   
                ui.badge() \
                    .bind_text_from(category, F_NUGGETS, lambda x: str(len(x or []))) \
                    .bind_visibility_from(category, F_NUGGETS) \
                    .props("floating transparent")

    with ui.tab_panels(tabs):
        for category in trendingmodel[F_CATEGORIES].values():
            with ui.tab_panel(category[F_NAME]):
                render_nuggets_as_expandable_list(category, settings).classes("w-full").style('flex: 1;') 

def load_trending_nuggets(viewmodel: dict):
    print("loading nuggets")
    for cat in viewmodel['trending'][F_CATEGORIES].values():
        # if not cat[F_NUGGETS]:
        load_trending_nuggets_for_category(cat, viewmodel['settings']['search'])

def _init_page_viewmodel(viewmodel):
    if not viewmodel.get('trending'):
        viewmodel['trending'] = {
            F_CATEGORIES: {cat: _create_category_viewmodel(cat) for cat in viewmodel['settings']['search']["topics"] or userops.get_default_preferences()},
            F_SELECTED: None
        }
    return viewmodel
        
def _create_category_viewmodel(cat: str):
    return {
        F_NAME:cat, 
        F_NUGGETS: None, 
        F_BEANS: None, 
        F_SELECTED: None,
        F_LISTVIEW: None
    }