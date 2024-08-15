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

    tags = beanops.trending_tags_and_highlights(None, None, DEFAULT_WINDOW, DEFAULT_LIMIT)
    if tags:
        render_tags([tag.tags for tag in tags])
    render_separator()   
    
    ui.label("📰 News").classes('text-h5 w-full')
    news = beanops.trending(None, None, None, (NEWS), DEFAULT_WINDOW, None, MAX_ITEMS_PER_PAGE) 
    if news:           
        render_beans_as_list(news, _render_bean_with_image).props("dense").classes("w-full")
    else:
        ui.label(messages.NOTHING_TRENDING)
    render_separator()

    ui.label("📱 Social Media").classes('text-h5 w-full')
    posts = beanops.trending(None, None, None, (POST), DEFAULT_WINDOW, None, MAX_ITEMS_PER_PAGE)    
    if posts:
        render_beans_as_list(posts, _render_bean_with_image).props("separator").classes("w-full")
    else:
        ui.label(messages.NOTHING_TRENDING)
    render_separator()

    ui.markdown(settings_markdown(settings['search']))    
    ui.label("Click on 📈 and 🔥 buttons for more trending stories by topics. \n\nClick on ⚙️ button to change topics and time window.").classes('text-caption')

async def render_trending_news(settings: dict, category: str, last_ndays: int):  
    render_shell(settings,"Trending News")
    render_text_banner(category)
    
    kinds = (NEWS, BLOG)
    total = beanops.count_beans(None, category, None, kinds, last_ndays, MAX_LIMIT)        
    if total: 
        tags_and_highlights = beanops.trending_tags_and_highlights(categories=category, kind=kinds, last_ndays=last_ndays, topn=DEFAULT_LIMIT)
        render_tags([tag.tags for tag in tags_and_highlights])
        render_separator()          
        ui.markdown("\n\n".join(["- "+item.highlights[0] for item in tags_and_highlights]))
        render_separator()
        await render_beans_page(category, kinds, last_ndays, total)
    else:
        ui.label(messages.NOTHING_TRENDING_IN%last_ndays)

async def render_hot_posts(settings: dict, category: str, last_ndays: int):  
    render_shell(settings, "Hot Posts")
    render_text_banner(category)
    
    kinds = (POST, COMMENT)
    total_beans = beanops.count_beans(None, category, None, kinds, last_ndays, MAX_LIMIT)     
    if total_beans: 
        tags_and_highlights = beanops.trending_tags_and_highlights(categories=category, kind=kinds, last_ndays=last_ndays, topn=DEFAULT_LIMIT)
        # top tags        
        render_tags([tag.tags for tag in tags_and_highlights])
        render_separator()         
        await render_beans_page(category, kinds, last_ndays, total_beans)
    else:
        ui.label(messages.NOTHING_HOT_IN%last_ndays)

async def render_beans_page(category, kinds, last_ndays, total):
    is_article = (NEWS in kinds) or (BLOG in kinds)
    beans = [] 
    @ui.refreshable
    def add_page():
        nonlocal beans
        beans += beanops.trending(None, category, None, kinds, last_ndays, len(beans), MAX_ITEMS_PER_PAGE)
        render_beans_as_list(beans, _render_bean_with_related_items).classes("w-full").props("dense" if is_article else "separator")
        if len(beans) < total:   
            ui.button("More Stories", on_click=add_page.refresh).props("unelevated icon-right=chevron_right")
    add_page()

def _render_bean_with_image(bean: Bean):
    is_article = bean.kind in (NEWS, BLOG)
    with ui.item(on_click=lambda: body_panel.set_visibility(not body_panel.visible)).classes("w-full" + (" border-[1px]" if is_article else "")) as view:  
        with ui.column():
            with ui.item().style("padding: 0px; margin: 0px"):
                if bean.image_url:
                    with ui.item_section().props("side"):
                        ui.image(bean.image_url).classes("w-36 h-36")
                with ui.item_section().props("top"):
                    ui.label(bean.highlights[0] if bean.highlights else (bean.title or bean.summary)).classes('text-bold')              
                    with ui.row(align_items="center").classes('text-caption'): 
                        if bean.source:
                            ui.markdown(f"🔗 [{bean.source}]({bean.url})")
                        if is_article and bean.created:
                            ui.label(f"📅 {date_to_str(bean.created)}") 
                        if not is_article and bean.comments:
                            ui.label(f"💬 {bean.comments}")
                        if not is_article and bean.likes:
                            ui.label(f"👍 {bean.likes}")   
            body_panel=ui.markdown(bean.summary)
            body_panel.set_visibility(False)
                
    return view
    
def _render_bean_with_related_items(bean: Bean): 
    is_article = bean.kind in (NEWS, BLOG)           
    @ui.refreshable
    def render_related_beans(load_items: bool):
        if load_items:
            render_beans_as_list(
                beanops.related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=DEFAULT_LIMIT),
                lambda bean: render_bean_as_card(bean, show_highlight=True).classes("w-full")).style("text-align: left; margin-left: 0px; padding-left: 0px;")

    bean_count = beanops.count_related(cluster_id=bean.cluster_id, url=bean.url, last_ndays=None, topn=DEFAULT_LIMIT)      
    with ui.card().props("flat " + (" bordered" if is_article else "")).classes("w-full") as view:         
        render_bean_banner(bean)  
        render_bean_body(bean, False)
        if bean_count:                     
            with ui.expansion(
                caption=f"{rounded_number_with_max(bean_count, DEFAULT_LIMIT)} related item(s)",
                group="group", 
                value=False, 
                on_value_change=lambda: render_related_beans.refresh(beans_panel.value)
                ).classes("w-full").style("text-align: right; padding: 2px; margin: 0px;") as beans_panel:                      
                render_related_beans(False)                       
    return view

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
        ui.navigate.to(make_url("/search", q=query, days=ndays))
    elif task == "trending":
        ui.navigate.to(make_url("/trending", category=query, days=ndays))
    else:
        ui.navigate.to(make_url("/search", q=prompt, days=ndays))

def render_shell(settings, current_tab="Home"):
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
            settings['search']['topics'] = sorted(settings['search']['topics'])
            with ui.tab(name="Trending News", label="", icon='trending_up').tooltip("Trending News"):
                BindableNavigationMenu(render_news_topic).bind_items_from(settings['search'], 'topics')            
            with ui.tab(name="Hot Posts", label="", icon="local_fire_department").tooltip("Hot Posts"):
                BindableNavigationMenu(render_post_topic).bind_items_from(settings['search'], 'topics')
            ui.tab(name="Search", label="", icon="search").tooltip("Search")

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
            with ui.expansion("Topics of Interest", caption="Select topics your are interesting in"):
                ui.select(options=espressops.DEFAULT_CATEGORIES, multiple=True).bind_value(settings['search'], 'topics').props("use-chips")
    
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
            "topics": espressops.DEFAULT_CATEGORIES
        },
        "connections": {
            config.REDDIT: None,
            config.SLACK: None
        }            
    }
