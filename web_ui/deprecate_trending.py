from shared import beanops, espressops
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, run
from icecream import ic
from .renderer import *

def load_trending_nuggets_for_category(category, settings):    
    nuggets = beanops.highlights(category[K_TEXT], settings['last_ndays'], settings['topn']) 
    category[F_NUGGETS] = [{'data': item} for item in nuggets]

def render_nuggets_as_expandable_list(viewmodel: dict, settings: dict):
    async def load_beans(nugget):
        if nugget[F_SELECTED]:
            nugget[F_BEANS] = await run.io_bound(beanops.get_beans_by_nugget, nugget['data'].id, tuple(settings['content_types']), settings['last_ndays'], settings['topn'])

    def render_nugget_as_expandable_item(nugget: dict):
        bean_count = beanops.count_beans_for_nugget(nugget['data'].id, tuple(settings['content_types']), settings['last_ndays'], settings['topn'])   
        with ui.item() as view:
            with ui.column(align_items="start", wrap=True):                        
                render_nugget_banner(nugget['data'])  
                with ui.expansion(
                        group="group", 
                        text=nugget['data'].description, 
                        caption=f"{bean_count_text(bean_count)} items",
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
            with ui.tab(category[K_TEXT], label=category[K_TEXT]):   
                ui.badge() \
                    .bind_text_from(category, F_NUGGETCOUNT, lambda x: str(x)) \
                    .bind_visibility_from(category, F_NUGGETCOUNT) \
                    .props("floating transparent")

    with ui.tab_panels(tabs):
        for category in trendingmodel[F_CATEGORIES].values():
            with ui.tab_panel(category[K_TEXT]):
                render_nuggets_as_expandable_list(category, settings).classes("w-full").style('flex: 1;') 

def load_trending(viewmodel: dict):
    print("loading nuggets")
    trendingmodel = viewmodel['trending']
    settings = viewmodel['settings']['search'] 
    if len(trendingmodel[F_CATEGORIES]) > 0:
        trendingmodel[F_SELECTED] = next(iter(trendingmodel[F_CATEGORIES].values()))[K_TEXT] 
    for cat in trendingmodel[F_CATEGORIES].values():
        cat[F_NUGGETCOUNT] = beanops.count_highlights(cat[K_TEXT], last_ndays=settings["last_ndays"], topn=settings['topn'])


def _init_page_viewmodel(viewmodel):
    if 'trending' not in viewmodel:
        if not viewmodel['settings']['search']["topics"]:
            viewmodel['settings']['search']["topics"] = espressops.get_topics(viewmodel.get('userid'), text_only=True) or espressops.get_topics(espressops.SYSTEM, text_only=True)
        viewmodel['trending'] = {
            F_CATEGORIES: {cat: _create_category_viewmodel(cat) for cat in viewmodel['settings']['search']["topics"]},
            F_SELECTED: None
        }
    return viewmodel
        
def _create_category_viewmodel(cat):
    return {
        K_TEXT:cat,
        F_NUGGETS: None, 
        F_BEANS: None, 
        F_SELECTED: None,
        F_LISTVIEW: None
    }