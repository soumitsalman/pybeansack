from itertools import chain
from app.shared.utils import *
from app.shared.messages import *
from app.shared.espressops import *
from app.shared.beanops import *
from app.pybeansack.datamodels import *
from app.web.renderer import *
from app.web.custom_ui import *
from app.pybeansack.utils import ndays_ago
from nicegui import ui
from icecream import ic
import inflect

KIND_LABELS = {NEWS: "News", POST: "Posts", BLOG: "Blogs"}
TRENDING, LATEST = "Trending", "Latest"
DEFAULT_SORT_BY = LATEST
SORT_BY_LABELS = {LATEST: LATEST, TRENDING: TRENDING}

REMOVE_FILTER = "remove-filter"
CONTENT_GRID_CLASSES = "w-full m-0 p-0 grid-cols-1 lg:grid-cols-2 xl:grid-cols-3"
BARISTAS_PANEL_CLASSES = "w-1/4 gt-xs"
TOGGLE_OPTIONS_PROPS = "unelevated rounded no-caps color=dark toggle-color=primary"

async def render_home(user):
    render_header(user)       
    with ui.grid().classes(CONTENT_GRID_CLASSES):
        # render trending blogs, posts and news
        for id, label in KIND_LABELS.items():
            with render_card_container(label, header_classes="text-h6 bg-dark", on_click=create_navigation_func("/beans", kind=id)).classes("bg-transparent"):
                # TODO: add a condition with count_beans to check if there are any beans. If not, then render a label with NOTHING TRENDING
                render_beans(user, lambda kind_id=id: beanops.get_trending_beans(embedding=None, accuracy=None, tags=None, kinds=kind_id, sources=None, last_ndays=1, start=0, limit=MAX_ITEMS_PER_PAGE)) \
                    if beanops.count_beans(query=None, embedding=None, accuracy=None, tags=None, kinds=id, sources=None, last_ndays=1, limit=1) else \
                        render_error_text(NOTHING_TRENDING)
        # render trending pages
        with render_card_container("Explore"):                
            render_barista_names(user, espressops.db.sample_baristas(5))
    render_footer()

async def render_trending_snapshot(user):
    render_header(user)
    with ui.grid().classes(CONTENT_GRID_CLASSES):
        baristas = espressops.db.get_baristas(user.following if user else espressops.DEFAULT_BARISTAS, projection=None)
        for barista in baristas:
            with render_card_container(barista.title, on_click=create_barista_navigate_func(barista), header_classes="text-wrap bg-dark").classes("bg-transparent"):
                if beanops.count_beans(query=None, embedding=barista.embedding, accuracy=barista.accuracy, tags=barista.tags, kinds=None, sources=None, last_ndays=1, limit=1):  
                    get_beans_func = lambda b=barista: beanops.get_newest_beans(
                        embedding=b.embedding, 
                        accuracy=b.accuracy, 
                        tags=b.tags, 
                        kinds=None, 
                        sources=b.sources, 
                        last_ndays=MIN_WINDOW, 
                        start=0, limit=MIN_LIMIT)
                    render_beans(user, get_beans_func)
                else:
                    render_error_text(NOTHING_TRENDING)                            
    render_footer()

inflect_engine = inflect.engine()
tags_banner_text = lambda tags: inflect_engine.join(tags) if tags else inflect_engine.join(list(KIND_LABELS.values()))

async def render_beans_page(user: User, must_have_tags: str|list[str], kind: str = DEFAULT_KIND): 
    if must_have_tags:
        must_have_tags = must_have_tags if isinstance(must_have_tags, list) else [must_have_tags]
    tags, sort_by = must_have_tags, DEFAULT_SORT_BY # starting default 

    def get_beans(start, limit):
        result = beanops.get_trending_beans(embedding=None, accuracy=None, tags=tags, kinds=kind, sources=None, last_ndays=None, start=start, limit=limit) \
            if sort_by == TRENDING else \
                beanops.get_newest_beans(embedding=None, accuracy=None, tags=tags, kinds=kind, sources=None, last_ndays=MIN_WINDOW, start=start, limit=limit)
        log("beans_page", user_id=user.email if user else None, tags=tags, kind=kind, sort_by=sort_by, start=start, urls=[bean.url for bean in result])
        return result

    def trigger_filter(filter_tags: list[str] = None, filter_kind: str = None, filter_sort_by: str = None):
        nonlocal tags, kind, sort_by
        if filter_tags == REMOVE_FILTER:
            tags = must_have_tags
        else:
            tags = [must_have_tags, filter_tags] if (must_have_tags and filter_tags) else (must_have_tags or filter_tags) # filter_tags == [] means there is no additional tag to filter with     
        if filter_kind:
            kind = filter_kind if filter_kind != REMOVE_FILTER else None
        if filter_sort_by:
            sort_by = filter_sort_by
        return get_beans
    
    ui.label(tags_banner_text(must_have_tags)).classes("text-h6")
    
    render_page_content(
        user, 
        lambda: beanops.get_tags(None, None, None, None, must_have_tags, None, None, None, 0, MAX_FILTER_TAGS), 
        trigger_filter,
        initial_kind=kind
    )

async def render_barista_page(user: User, barista: Barista):    
    tags, kind, sort_by = barista.tags, DEFAULT_KIND, DEFAULT_SORT_BY # starting default values

    def get_beans(start, limit):
        # if specific urls are provided (applicable for pages that are from user shares)
        if barista.urls:
            result = beanops.get_beans(urls=barista.urls, tags=tags, kinds=kind, sources=barista.sources, last_ndays=None, sort_by=LATEST_AND_TRENDING, start=start, limit=limit) \
                if sort_by == TRENDING else \
                    beanops.get_beans(urls=barista.urls, tags=tags, kinds=kind, sources=barista.sources, last_ndays=MIN_WINDOW, sort_by=NEWEST_AND_TRENDING, start=start, limit=limit)
        else:
            result = beanops.get_trending_beans(embedding=barista.embedding, accuracy=barista.accuracy, tags=tags, kinds=kind, sources=barista.sources, last_ndays=barista.last_ndays, start=start, limit=limit) \
                if sort_by == TRENDING else \
                    beanops.get_newest_beans(embedding=barista.embedding, accuracy=barista.accuracy, tags=tags, kinds=kind, sources=barista.sources, last_ndays=MIN_WINDOW, start=start, limit=limit)
        log("barista_page", user_id=user.email if user else None, page_id=barista.id, tags=tags, kind=kind, sort_by=sort_by, start=start, urls=[bean.url for bean in result])
        return result
    
    def trigger_filter(filter_tags: list[str] = None, filter_kind: str = None, filter_sort_by: str = None) -> Callable:
        nonlocal tags, kind, sort_by
        if filter_tags == REMOVE_FILTER: # explicitly mentioning is not None is important because that is the default value
            tags = barista.tags
        else:
            tags = [barista.tags, filter_tags] if (barista.tags and filter_tags) else (barista.tags or filter_tags) # filter_tags == [] means there is no additional tag to filter with
        if filter_kind:
            kind = filter_kind if filter_kind != REMOVE_FILTER else None
        if filter_sort_by:
            sort_by = filter_sort_by
        return get_beans
        
    async def toggle_publish():
        if espressops.db.is_published(barista.id):
            espressops.db.unpublish(barista.id)
            log("unpublished", user_id=user_id(user), page_id=barista.id)
        else:
            espressops.db.publish(barista.id)
            log("published", user_id=user_id(user), page_id=barista.id)
            
    async def toggle_follow():
        if barista.id not in user.following:
            espressops.db.follow_barista(user.email, barista.id)
            log("followed", user_id=user_id(user), page_id=barista.id)
        else:
            espressops.db.unfollow_barista(user.email, barista.id)
            log("unfollowed", user_id=user_id(user), page_id=barista.id)
            
    with ui.label(barista.title).classes("text-h6"):        
        if user:
            has_publish_permission = barista.owner == user.email
            has_unfollow_permission = barista.owner != user.email
            with ui.button(icon="more_vert").props("flat").classes("q-ml-md"):
                with ui.menu():  
                    if has_publish_permission:
                        with ui.item("Public"):
                            ui.switch(value=barista.public, on_change=toggle_publish).props("flat checked-icon=public unchecked-icon=public_off")

                    if has_unfollow_permission:
                        with ui.item("Follow"):
                            ui.switch(value=barista.id in user.following, on_change=toggle_follow).props("flat checked-icon=playlist_add_check")

                    with ui.menu_item("Pour a Filtered Cup", on_click=lambda: ui.notify("Coming soon")):
                        ui.avatar(icon="filter_list", color="transparent")
    
    render_page_content(
        user, 
        lambda: beanops.get_tags(barista.urls, None, barista.embedding, barista.accuracy, barista.tags, None, barista.sources, None, 0, MAX_FILTER_TAGS), 
        trigger_filter,
        initial_kind=kind
    )

def render_page_content(user, get_filter_tags_func: Callable, trigger_filter_func: Callable, initial_kind: str):
    @ui.refreshable
    def render_beans_panel(filter_tags: list[str] = None, filter_kind: str = None, filter_sort_by: str = None):        
        return render_beans_as_extendable_list(
            user, 
            trigger_filter_func(filter_tags, filter_kind, filter_sort_by), 
            ui.grid().classes(CONTENT_GRID_CLASSES)
        ).classes("w-full")

    render_header(user)  
            
    render_filter_tags(
        load_tags=get_filter_tags_func, 
        on_selection_changed=lambda selected_tags: render_beans_panel.refresh(filter_tags=(selected_tags or REMOVE_FILTER))).classes("w-full")
    
    with ui.row(wrap=False, align_items="stretch").classes("w-full"):
        ui.toggle(
            options=KIND_LABELS,
            value=initial_kind,
            on_change=lambda e: render_beans_panel.refresh(filter_kind=(e.sender.value or REMOVE_FILTER))).props(TOGGLE_OPTIONS_PROPS)
        
        ui.toggle(
            options=SORT_BY_LABELS, 
            value=DEFAULT_SORT_BY, 
            on_change=lambda e: render_beans_panel.refresh(filter_sort_by=e.sender.value)).props("unelevated rounded no-caps color=dark")
    render_beans_panel(filter_tags=None, filter_kind=None, filter_sort_by=None).classes("w-full")
    render_footer()

SAVED_PAGE = "saved_page"
SEARCH_PAGE_TABS = {**KIND_LABELS, **{SAVED_PAGE: "Pages"}}
# NOTE: if query length is small think of it as a domain/genre
prep_query = lambda query: f"Domain / Genre / Category / Topic: {query}" if len(query.split()) > 3 else query
async def render_search(user: User, query: str, accuracy: float):
    tags, kind, last_ndays = None, DEFAULT_KIND, DEFAULT_WINDOW
    
    def get_beans(start, limit):
        result = beanops.vector_search_beans(query=prep_query(query), accuracy=accuracy, tags=tags, kinds=kind, sources=None, last_ndays=last_ndays, start=start, limit=limit)
        log("search", user_id=user.email if user else None, query=query, accuracy=accuracy, tags=tags, kind=kind, last_ndays=last_ndays, start=start, urls=[bean.url for bean in result])
        return result
    
    @ui.refreshable
    def render_result_panel(filter_accuracy: float = None, filter_tags: str|list[str] = None, filter_kind: str = None, filter_last_ndays: int = None):
        nonlocal accuracy, tags, kind, last_ndays        

        if filter_accuracy:
            accuracy = filter_accuracy        
        if filter_tags:
            tags = filter_tags if filter_tags != REMOVE_FILTER else None
        if filter_kind:    
            kind = filter_kind if filter_kind != REMOVE_FILTER else None
        if filter_last_ndays:
            last_ndays = filter_last_ndays

        if kind == SAVED_PAGE and query:
            result = espressops.db.search_baristas(query)
            log("search", user_id=user.email if user else None, query=query, tags=tags, kind=kind)
            return render_barista_names(user, result) \
                if result else \
                    render_error_text(NOTHING_FOUND)
        
        return render_paginated_beans(
            user, 
            get_beans, 
            lambda: beanops.count_beans(query=query, embedding=None, accuracy=accuracy, tags=tags, kinds=kind, sources=None, last_ndays=last_ndays, limit=MAX_LIMIT)).classes("w-full")               

    render_header(user)
    trigger_search = lambda: ui.navigate.to(create_search_target(search_input.value))
    with ui.input(placeholder=SEARCH_PLACEHOLDER, value=query) \
        .props('rounded outlined input-class=mx-3').classes('w-full self-center lt-sm') \
        .on('keydown.enter', trigger_search) as search_input:
        ui.button(icon="send", on_click=trigger_search).bind_visibility_from(search_input, 'value').props("flat dense")  

    if query:             
        with ui.grid(columns=2).classes("w-full"):                         
            with ui.label("Accuracy").classes("w-full"):
                accuracy_filter = ui.slider(
                    min=0.1, max=1.0, step=0.05, 
                    value=(accuracy or DEFAULT_ACCURACY), 
                    on_change=debounce(lambda: render_result_panel.refresh(filter_accuracy=accuracy_filter.value), 1.5)).props("label-always")

            with ui.label().classes("w-full") as last_ndays_label:
                last_ndays_filter = ui.slider(
                    min=-30, max=-1, step=1, value=-last_ndays,
                    on_change=debounce(lambda: render_result_panel.refresh(filter_last_ndays=-last_ndays_filter.value), 1.5))
                last_ndays_label.bind_text_from(last_ndays_filter, 'value', lambda x: f"Since {naturalday(ndays_ago(-x))}")

        kind_filter = ui.toggle(
            options=SEARCH_PAGE_TABS, 
            value=DEFAULT_KIND, 
            on_change=lambda: render_result_panel.refresh(filter_kind=kind_filter.value or REMOVE_FILTER)).props("unelevated rounded no-caps color=dark toggle-color=primary").classes("w-full")               
        
        render_filter_tags(
            load_tags=lambda: beanops.get_tags(None, prep_query(query), None, accuracy, None, None, None, None, 0, MAX_FILTER_TAGS), 
            on_selection_changed=lambda selected_tags: render_result_panel.refresh(filter_tags=(selected_tags or REMOVE_FILTER))).classes("w-full")
        
        render_result_panel(filter_accuracy=None, filter_tags=None, filter_kind=None, filter_last_ndays=None)
    # TODO: fill it up with popular searches
    render_footer()

async def render_registration(userinfo: dict):
    render_header(None)

    async def success():
        espressops.db.create_user(userinfo)
        ui.navigate.to("/")

    with ui.card(align_items="stretch").classes("self-center"):
        ui.label("You look new!").classes("text-h4")
        ui.label("Let's get you signed up.").classes("text-caption")
        
        with ui.row(wrap=False).classes("justify-between"):
            with ui.column(align_items="start"):
                ui.label("User Agreement").classes("text-h6")
                ui.link("What is Espresso", "https://github.com/soumitsalman/espresso/blob/main/README.md", new_tab=True)
                ui.link("Terms of Use", "https://github.com/soumitsalman/espresso/blob/main/docs/terms-of-use.md", new_tab=True)
                ui.link("Privacy Policy", "https://github.com/soumitsalman/espresso/blob/main/docs/privacy-policy.md", new_tab=True)                
            ui.separator().props("vertical")
            with ui.column(align_items="end"):   
                if "picture" in userinfo:
                    ui.image(userinfo["picture"]).classes("w-24")  
                ui.label(userinfo["name"]).classes("text-bold")
                ui.label(userinfo["email"]).classes("text-caption")
        
        agreement = ui.checkbox(text="I have read and understood every single word in each of the links above. I agree to the terms and conditions.") \
            .tooltip("We are legally obligated to ask you this question. Please read the documents to reduce our chances of going to jail.")
        with ui.row():
            ui.button("Agreed", color="primary", icon="thumb_up", on_click=success).bind_enabled_from(agreement, "value").props("unelevated")
            ui.button('Nope!', color="negative", icon="cancel", on_click=lambda: ui.navigate.to("/")).props("outline")

async def render_doc(user: User, doc_id: str):
    render_header(user)
    with open(f"./docs/{doc_id}", 'r') as file:
        ui.markdown(file.read()).classes("w-full md:w-2/3 lg:w-1/2  self-center")
    render_footer()
