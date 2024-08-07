from shared import beanops, espressops, config, messages
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

    ui.markdown(settings_markdown(settings['search']))
    # nuggets = beanops.trending_tags(1, 10)
    # with ui.row().classes("gap-0"):
    #     [render_tag(nugget) for nugget in nuggets]

async def render_trending_news(settings: dict, category: str, last_ndays: int):  
    _render_shell(settings,"Trending News")
    render_text_banner(category)
    
    kind = (NEWS, BLOG)
    total_beans = beanops.count_beans(None, category, None, kind, last_ndays, MAX_LIMIT)        
    
    if total_beans: 
        tags_and_highlights = beanops.trending_tags_and_highlights(categories=category, kind=kind, last_ndays=last_ndays, topn=DEFAULT_LIMIT)
        # top tags        
        render_tags([tag.tags for tag in tags_and_highlights])
        ui.separator()          
        ui.markdown("\n\n".join(["- "+item.highlights[0] for item in tags_and_highlights]))
        ui.separator()
        await _render_beans_page(total_beans, category, kind, last_ndays)
    else:
        ui.label(messages.NOTHING_TRENDING_IN%last_ndays)

async def render_hot_posts(settings: dict, category: str, last_ndays: int):  
    _render_shell(settings, "Hot Posts")
    render_text_banner(category)
    
    kind = (POST, COMMENT)
    total_beans = beanops.count_beans(None, category, None, kind, last_ndays, MAX_LIMIT)     
    if total_beans: 
        tags_and_highlights = beanops.trending_tags_and_highlights(categories=category, kind=kind, last_ndays=last_ndays, topn=DEFAULT_LIMIT)
        # top tags        
        render_tags([tag.tags for tag in tags_and_highlights])
        ui.separator()          
        await _render_beans_page(total_beans, category, kind, last_ndays)
    else:
        ui.label(messages.NOTHING_HOT_IN%last_ndays)

@ui.refreshable
async def _render_beans_page(total_beans, category, kind, last_ndays):
    # nonlocal current_page
    beans, next_page = ui.state(beanops.trending(None, category, None, kind, last_ndays, 0, MAX_ITEMS_PER_PAGE))
    with ui.list().classes("w-full"):
        [_render_bean_with_related_items(bean) for bean in beans]                
    if len(beans) < total_beans:   
        ui.button("More", 
            on_click=lambda: next_page(beans+beanops.trending(None, category, None, kind, last_ndays, len(beans)+MAX_ITEMS_PER_PAGE, MAX_ITEMS_PER_PAGE))) \
            .props("unelevated icon-right=chevron_right")
    
def _render_bean_with_related_items(bean: Bean):            
    @ui.refreshable
    def render_related_beans(load_items: bool):
        if load_items:
            render_beans_as_list(
                beanops.related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=DEFAULT_LIMIT),
                lambda bean: render_bean_as_card(bean, show_highlight=True).style("text-align: left;"))

    bean_count = beanops.count_related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=DEFAULT_LIMIT)      
    with ui.card().classes("no-shadow border-[1px] w-full") as view:         
        render_bean_banner(bean)  
        render_bean_body(bean, False)
        if bean_count:                     
            with ui.expansion(
                caption=f"{rounded_number_with_max(bean_count, DEFAULT_LIMIT)} related item(s)",
                group="group", 
                value=False, 
                on_value_change=lambda: render_related_beans.refresh(beans_panel.value)
                ).classes("w-full").style("text-align: right") as beans_panel:                      
                render_related_beans(False)                       
    return view

async def render_search(settings, query: str, keyword: str, kind, last_ndays: int, topn: int):
    _render_shell(settings, "Search") 
    process_prompt = lambda: _trigger_search(settings, prompt_input.value)   
    with ui.input(placeholder=PLACEHOLDER, autocomplete=EXAMPLE_OPTIONS).on('keydown.enter', process_prompt) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center') as prompt_input:
        ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
    
    kind = tuple(kind) if kind else None
    async def _run_search():        
        if keyword or query:
            return (beanops.count_beans(query=query, categories=None, tags=keyword, kind=kind, last_ndays=last_ndays, topn=topn),
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
        ui.navigate.to(make_url("/search", q=query, days=ndays, topn=limit))
    elif task == "trending":
        ui.navigate.to(make_url("/trending", category=query, days=ndays, topn=limit))
    else:
        ui.navigate.to(make_url("/search", q=prompt, days=ndays, topn=limit))

def _render_shell(settings, current_tab="Home"):
    # set themes  
    ui.colors(secondary=SECONDARY_COLOR)
    ui.add_css(content=CSS)
    
    def render_news_topic(topic):
        return (topic, 
                make_url("/trending", category=topic, days=settings['search']['last_ndays']),
                beanops.count_beans(query=None, categories=topic, tags=None, kind=(NEWS, BLOG), last_ndays=settings['search']['last_ndays'], topn=MAX_LIMIT))

    def render_post_topic(topic):
        return (topic, 
                make_url("/hot", category=topic, days=settings['search']['last_ndays']),
                beanops.count_beans(query=None, categories=topic, tags=None, kind=(POST, COMMENT), last_ndays=settings['search']['last_ndays'], topn=MAX_LIMIT))

    def navigate(selected_tab):
        if selected_tab == "Home":
            ui.navigate.to("/")
        elif selected_tab == "Search":
            ui.navigate.to("/search")

    # header
    with ui.header().classes(replace="row"):
        with ui.avatar(square=True):
            ui.image("images/cafecito.png")
        with ui.tabs(on_change=lambda: navigate(tab_selector.value), value=current_tab) as tab_selector:
            ui.tab(name="Home", label="", icon="home").tooltip("Home")
            ui.tab(name="Search", label="", icon="search").tooltip("Search")
            with ui.tab(name="Trending News", label="", icon='trending_up').tooltip("Trending News"):
                BindableNavigationMenu(render_news_topic).bind_items_from(settings['search'], 'topics')            
            with ui.tab(name="Hot Posts", label="", icon="local_fire_department").tooltip("Hot Posts"):
                BindableNavigationMenu(render_post_topic).bind_items_from(settings['search'], 'topics')

        ui.space()
        ui.button(on_click=lambda: settings_drawer.toggle(), icon="settings").props('flat color=white').classes("self-right")

    # settings
    with ui.right_drawer(elevated=True, value=False) as settings_drawer:
        _render_settings(settings) 

def _render_settings(settings):   
    with ui.list():
        ui.item_label('Default Settings').classes("text-subtitle1")
        with ui.item():
            with ui.item_section().bind_text_from(settings['search'], "last_ndays", lambda x: f"Last {x} days"):
                ui.slider(min=MIN_WINDOW, max=MAX_WINDOW, step=1).bind_value(settings['search'], "last_ndays")
        with ui.item():
            with ui.item_section().bind_text_from(settings['search'], "topn", lambda x: f"Top {x} results"):
                ui.slider(min=MIN_LIMIT, max=MAX_LIMIT, step=1).bind_value(settings['search'], "topn")
        with ui.item():
            with ui.expansion("Topics of Interest", caption="Select topics your are interesting in"):
                ui.select(options=beanops.get_categories(), multiple=True).bind_value(settings['search'], 'topics').props("use-chips")
    
    ui.separator()

    with ui.column(align_items="stretch"):
        ui.label('Connections').classes("text-subtitle1")
        ui.switch(text="Slack")
        ui.switch(text="Reddit")
        ui.switch(text="LinkedIn")

def create_default_settings():
    return {
        "search": {
            "last_ndays": DEFAULT_WINDOW,
            "topn": DEFAULT_LIMIT,
            "topics": beanops.get_categories()
        },
        "connections": {
            config.REDDIT: None,
            config.SLACK: None
        }            
    }
