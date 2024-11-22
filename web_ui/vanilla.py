from connectors import redditor
import env
from shared.utils import *
from shared.messages import *
from shared import beanops, espressops, prompt_parser
from pybeansack.datamodels import *
from web_ui.custom_ui import *
from nicegui import ui, background_tasks, run
from icecream import ic
from .renderer import *

APP_NAME = env.app_name()
CONTENT_GRID_CLASSES = "w-full grid-cols-1 md:grid-cols-2"
BARISTAS_PANEL_CLASSES = "w-1/4 gt-xs"

# shows everything that came in within the last 24 hours
async def render_home(user):
    render_header(user)
    with ui.row(wrap=False).classes("w-full"): 
        render_baristas_panel(user).classes(BARISTAS_PANEL_CLASSES)
        with ui.grid().classes(CONTENT_GRID_CLASSES):
            # render trending blogs, posts and news
            for kind in DEFAULT_KINDS:
                with render_card_container(kind[K_TITLE]):
                    # TODO: add a condition with count_beans to check if there are any beans. If not, then render a label with NOTHING TRENDING
                    render_beans(user, lambda kind=kind: beanops.get_trending_beans(tags=None, kinds=kind[K_ID], sources=None, last_ndays=1, start=0, limit=MAX_ITEMS_PER_PAGE)) \
                        if beanops.count_beans(query=None, accuracy=None, urls=None, tags=None, kinds=kind[K_ID], last_ndays=1, limit=1) else \
                            render_error_text(NOTHING_TRENDING)
            # render trending pages
            with render_card_container("Explore"):                
                render_baristas(user, random.sample(espressops.get_baristas(None), 5))
    render_footer()

async def render_barista_snapshots(user):
    baristas = espressops.get_following_baristas(user) or espressops.get_baristas(utils.DEFAULT_BARISTAS)
    
    async def load_and_render(holder: ui.element, barista):
        beans = [(await run.io_bound(beanops.get_trending_beans, tags=barista.tags, kinds=kind[K_ID], sources=None, last_ndays=1, start=0, limit=1)) for kind in DEFAULT_KINDS]
        beans = [bean[0] for bean in beans if bean]
        with holder:
            render_swipable_beans(user, beans).classes("w-full") \
                if beans else \
                    render_error_text(NOTHING_TRENDING)

    render_header(user)
    with ui.row(wrap=False).classes("w-full"): 
        render_baristas_panel(user).classes(BARISTAS_PANEL_CLASSES)
        with ui.grid().classes(CONTENT_GRID_CLASSES):
            for barista in baristas:
                holder = render_card_container(barista.title, on_click=create_barista_route(barista))
                background_tasks.create_lazy(load_and_render(holder, barista), name=f"trending-{now()}")
                            
    render_footer()

async def render_barista_servings(user, barista_id: str):
    barista = espressops.get_barista(barista_id)
    render_header(user)
    with ui.row(wrap=False).classes("w-full"): 
        render_baristas_panel(user).classes(BARISTAS_PANEL_CLASSES)
        with ui.column(align_items="stretch").classes("w-full m-0 p-0"):  
            with ui.row(wrap=False, align_items="start").classes("justify-between q-mb-md w-full"):
                ui.label(barista.title).classes("text-h4")
                if user:
                    ui.button("Follow", icon="add").props("unelevated")
            # checking if there is actually any content of this kind        
            bean_exists = {kind[K_ID]: beanops.count_beans(query=None, accuracy=None, urls=None, tags=barista.tags, kinds=kind[K_ID], last_ndays=None, limit=1) for kind in DEFAULT_KINDS}
            with ui.tabs().classes("w-full") as tabs:
                [ui.tab(name=kind[K_ID], label=kind[K_TITLE]) for kind in DEFAULT_KINDS if bean_exists[kind[K_ID]]]
            with ui.tab_panels(tabs, value=DEFAULT_KINDS[0][K_ID]).classes("w-full rounded-borders"):
                for kind in DEFAULT_KINDS:
                    if bean_exists[kind[K_ID]]:
                        with ui.tab_panel(kind[K_ID]).classes("w-full p-0 overflow-hidden").classes("w-full"):
                            # TODO: render_appendable_tags with select action
                            render_appendable_beans(user, lambda start, limit, kind=kind: beanops.get_trending_beans(tags=barista.tags, kinds=kind[K_ID], sources=None, last_ndays=None, start=start, limit=limit)).classes("w-full m-0 p-0")                                
            if not any(bean_exists.values()):
                render_error_text(NOTHING_TRENDING)
    render_footer()

async def render_search(user, query: str, accuracy: float, tags: str|list[str], kinds: str|list[str]):
    process_prompt = lambda: ui.navigate.to(create_navigation_target("/search", q=prompt_input.value, kind=kinds_panel.value, acc=accuracy_panel.value))
    # search_func = lambda start, limit: beanops.search_beans(query=query, accuracy=accuracy, tags=tags, kinds=kinds, sources=None, last_ndays=None, start=start, limit=limit)

    async def search_and_render(holder: ui.element):
        items = await run.cpu_bound(beanops.search_beans, query=query, accuracy=accuracy, tags=tags, kinds=kinds, sources=None, last_ndays=None, start=0, limit=MAX_LIMIT)
        holder.clear()
        with holder:            
            render_paginated_beans(user, lambda start, limit: items[start:start+limit], len(items)).classes("rounded-borders") \
                if items else \
                    render_error_text(NOTHING_FOUND)

    render_header(user)
    with ui.row(wrap=False).classes("w-full"):
        render_baristas_panel(user).classes(BARISTAS_PANEL_CLASSES)
        # render the search input
        with ui.column(align_items="stretch").classes("w-full m-0 p-0"):
            with ui.input(placeholder=CONSOLE_PLACEHOLDER, autocomplete=CONSOLE_EXAMPLES).on('keydown.enter', process_prompt) \
                .props('rounded outlined input-class=mx-3').classes('w-full self-center') as prompt_input:
                ui.button(icon="send", on_click=process_prompt).bind_visibility_from(prompt_input, 'value').props("flat dense")
            
            # TODO: make the knob be inside the input box and the expansion be a dialog/drawer
            with ui.expansion(text="Knobs").props("expand-icon=tune expanded-icon=tune").classes("w-full text-right text-caption"):
                with ui.row(wrap=False, align_items="stretch").classes("w-full border-[1px]").style("border-radius: 5px; padding-left: 1rem;"):                
                    with ui.label("Accuracy").classes("w-full text-left"):
                        accuracy_panel=ui.slider(min=0.1, max=1.0, step=0.05, value=(accuracy or DEFAULT_ACCURACY)).props("label-always")
                    kinds_panel = ui.toggle(options={NEWS:"News", BLOG:"Blogs", POST:"Posts", None:"All"}).props("unelevated no-caps")
            
            # render this ONLY if there is a query or tag present or else the banner will be empty
            if query or tags: 
                ui.label(query or (tags if isinstance(tags, str) else ", ".join(tags))).classes("text-h4 banner")
                tab_names = ["Beans", "Baristas"] if not kinds else ["Beans"]
                
                with ui.tabs().classes("w-full") as tabs:                    
                    [ui.tab(name=tab_name, label=tab_name) for tab_name in tab_names]
                with ui.tab_panels(tabs, value=tab_names[0]).classes("w-full"):
                    for tab_name in tab_names:
                        with ui.tab_panel(name=tab_name).classes("w-full"):
                            if tab_name == "Beans":
                                background_tasks.create_lazy(search_and_render(render_skeleton_beans(3).classes("w-full")), name=f"search-{now()}") 
                            else:
                                res = espressops.search_baristas(query or tags)
                                render_baristas(user, res).classes("rounded-borders w-full") if res else render_error_text(NOTHING_FOUND)
    render_footer()

async def render_doc(user, doc_id):
    render_header(user)
    with open(f"./docs/{doc_id}", 'r') as file:
        ui.markdown(file.read()).classes("w-full md:w-2/3 lg:w-1/2  self-center")
    render_footer()


# def _render_beans_page(user, banner: str, urls: list[str], categories: str|list[str], last_ndays: int|None):
#     selected_tags = []    
#     async def on_tag_select(tag: str, selected: bool):
#         selected_tags.append(tag) if selected else selected_tags.remove(tag)
#         load_beans() 

#     if banner:
#         render_banner_text(banner)

#     tags_holder = ui.row(align_items="center").classes("w-full gap-0")   
#     render_separator()

#     count_holders, bean_holders = [], []
#     with ui.tabs().props("dense").classes("w-full") as tab_headers:
#         for tab in TRENDING_TABS:
#             with ui.tab(name=tab['name'], label=""):
#                 with ui.row(wrap=False, align_items="stretch"):
#                     ui.label(tab['label'])                    
#                     count_holders.append(ui.badge("...").props("transparent"))
#     with ui.tab_panels(tabs=tab_headers, animated=True, value=TRENDING_TABS[0]['name']).props("swipeable").classes("w-full h-full m-0 p-0"):  
#         for tab in TRENDING_TABS:
#             with ui.tab_panel(name=tab['name']).classes("w-full h-full m-0 p-0") as panel:
#                 render_skeleton_beans(3)   
#                 bean_holders.append(panel)

#     def load_beans():
#         for i, tab in enumerate(TRENDING_TABS):   
#             background_tasks.create_lazy(
#                 _load_counter(count_holders[i], urls, categories, selected_tags, tab['kinds'], last_ndays), 
#                 name=f"trending-{tab['name']}-count"
#             ) 
#             background_tasks.create_lazy(
#                 _load_trending_beans(bean_holders[i], urls, categories, selected_tags, tab['kinds'], last_ndays, user), 
#                 name=f"trending-{tab['name']}-beans"
#             )    

#     # load the tags
#     background_tasks.create_lazy(
#         _load_trending_tags(tags_holder, urls, categories, None, last_ndays, on_tag_select), 
#         name=f"trending-tags-{categories}")
#     load_beans()
 
# async def _load_counter(badge: ui.badge, urls, categories, tags, kinds, last_ndays):
#     count = beanops.count_beans(None, urls, categories, tags, kinds, last_ndays, MAX_LIMIT+1)
#     badge.set_visibility(count > 0)
#     badge.set_text(rounded_number_with_max(count, MAX_LIMIT))

# async def _load_trending_tags(tags_panel: ui.element, urls, categories, kinds, last_ndays, on_tag_select):
#     start_index, topn = 0, DEFAULT_LIMIT
#     async def render_more(clear_panel: bool = False):
#         # retrieve 1 more than needed to check for whether to show the 'more' button 
#         # this way I can check if there are more beans left in the pipe
#         # because if there no more beans left no need to show the 'more' button
#         nonlocal start_index
#         tags = beanops.get_trending_tags(urls, categories, kinds, last_ndays, start_index, topn+1)
#         start_index += topn
#         # clear the pagen of older stuff
#         if clear_panel:
#             tags_panel.clear()
#         # remove the more_button so that we can insert the new tags
#         if tags_panel.slots['default'].children:
#             tags_panel.slots['default'].children[-1].delete()

#         with tags_panel:
#             render_tags_as_chips([tag.tags for tag in tags[:topn]], on_select=on_tag_select)
#             if len(tags) > topn:
#                 ui.chip(text="More", icon="more_horiz", on_click=render_more).props("unelevated dense")

#     with tags_panel:
#         render_skeleton_tags(3)

#     await render_more(True)      
     
# async def _load_trending_beans(holder: ui.element, urls, categories, tags, kinds, last_ndays, for_user):     
#     is_article = (NEWS in kinds) or (BLOG in kinds) 
#     start_index = 0
    
#     def get_beans():
#         nonlocal start_index
#         # retrieve 1 more than needed to check for whether to show the 'more' button 
#         # this way I can check if there are more beans left in the pipe
#         # because if there no more beans left no need to show the 'more' button
#         beans = beanops.trending(urls, categories, tags, kinds, last_ndays, start_index, MAX_ITEMS_PER_PAGE+1)
#         ic([bean.updated for bean in beans], [bean.created for bean in beans], [bean.trend_score for bean in beans])
#         start_index += MAX_ITEMS_PER_PAGE
#         return beans[:MAX_ITEMS_PER_PAGE], (len(beans) > MAX_ITEMS_PER_PAGE)

#     def render_beans(beans: list[Bean], panel: ui.list):
#         with panel:        
#             for bean in beans:                
#                 with ui.item().classes(bean_item_class(is_article)).style(bean_item_style):
#                     render_expandable_bean(for_user, bean, True)

#     async def next_page():
#         nonlocal start_index, more_button
#         with disable_button(more_button):
#             beans, more = get_beans()
#             render_beans(beans, beans_panel)         
#         if not more:
#             more_button.delete()
    
#     beans, more = get_beans()
#     holder.clear()
#     with holder:   
#         if not beans:
#             ui.label(BEANS_NOT_FOUND)
#             return             
#         beans_panel = ui.list().props("dense" if is_article else "separator").classes("w-full")      
#         render_beans(beans, beans_panel)
#         with ui.row(wrap=False, align_items="stretch").classes("w-full"):
#             if more:
#                 more_button = ui.button("More Stories", on_click=next_page).props("unelevated icon-right=chevron_right")
#             if for_user:
#                 ui.button("Follow", on_click=lambda: ui.notify(NOT_IMPLEMENTED)).props("icon-right=add")


       
# def render_shell(settings, user, current_tab: str, render_func: Callable):
#     render_header(user)

#     render_func()
    
    # render_topics_menu = lambda topic: (espressops.category_label(topic), lambda: ui.navigate.to(make_navigation_target(f"/page/{topic}", ndays=settings['search']['last_ndays'])))

    # def navigate(selected_tab):
    #     if selected_tab == "Home":
    #         ui.navigate.to("/")
    #     if selected_tab == "Search":
    #         ui.navigate.to("/search")

    # # settings
    # with ui.right_drawer(elevated=True, value=False) as settings_drawer:
    #     _render_settings(settings, user)

    # # header
    # with render_header():  
    #     with ui.tabs(on_change=lambda e: navigate(e.sender.value), value=current_tab).style("margin-right: auto;"):
    #         ui.tab(name="Home", label="", icon="home").tooltip("Home")           
    #         settings['search']['topics'] = sorted(settings['search']['topics'])
    #         with ui.tab(name="Trending", label="", icon='trending_up').tooltip("Trending News & Posts"):
    #             BindableNavigationMenu(render_topics_menu).bind_items_from(settings['search'], 'topics') 
    #         ui.tab(name="Search", label="", icon="search").tooltip("Search")
                 
    #     ui.label(APP_NAME).classes("text-bold app-name")
   
    #     if not user:
    #         with ui.button(icon="login").props("flat stretch color=white").style("margin-left: auto;"):
    #             with ui.menu().classes("text-bold"):                       
    #                 with ui.menu_item(text="Continue with Reddit", on_click=lambda: ui.navigate.to("/reddit/login")).style("border-radius: 20px; border-color: #FF4500;").classes("border-[1px] m-1"):
    #                     ui.avatar(REDDIT_ICON_URL, color="transparent")
    #                 with ui.menu_item(text="Continue with Slack", on_click=lambda: ui.navigate.to('/web/slack/login')).style("border-radius: 20px; border-color: #8E44AD;").classes("border-[1px] m-1"):
    #                     ui.avatar(SLACK_ICON_URL, color="transparent")
    #         ui.button(on_click=settings_drawer.toggle, icon="settings", color="secondary").props("flat stretch color=white")
    #     else:
    #         ui.button(on_click=settings_drawer.toggle, icon="img:"+user.get(espressops.IMAGE_URL) if user.get(espressops.IMAGE_URL) else "settings").props("flat stretch color=white").style("margin-left: auto;")

    # with ui.column(align_items="stretch").classes("responsive-container"):
    #     render_func()
    #     render_separator()
    #     render_footer_text() 

# def _render_user_profile(settings: dict, user: dict):
#     user_connected = lambda source: bool(user and (source in user.get(espressops.CONNECTIONS, "")))

#     def update_connection(source, connect):        
#         if user and not connect:
#             # TODO: add a dialog
#             espressops.remove_connection(user, source)
#             del user[espressops.CONNECTIONS][source]
#         else:
#             ui.navigate.to(f"/{source}/login")
    
#     ui.link("u/"+user[K_ID], target=f"/channel/{user[K_ID]}").classes("text-bold")
#     with ui.row(wrap=False, align_items="stretch").classes("w-full gap-0"):  
#         with ui.column(align_items="stretch"):
#             # sequencing of bind_value and on_value_change is important.
#             # otherwise the value_change function will be called every time the page loads
#             ui.switch(text="Reddit", value=user_connected(REDDIT), on_change=lambda e: update_connection(REDDIT, e.sender.value)).tooltip("Link/Unlink Connection")
#             ui.switch(text="Slack", value=user_connected(SLACK), on_change=lambda e: update_connection(SLACK, e.sender.value)).tooltip("Link/Unlink Connection")
#         ui.space()
#         with ui.column(align_items="stretch").classes("gap-1"):
#             _render_user_image(user)
#             ui.button(text="Log out", icon="logout", color="negative", on_click=lambda: ui.navigate.to("/logout")).props("dense unelevated size=sm")
    
# def _render_settings(settings: dict, user: dict):  
#     async def delete_user():     
#         # TODO: add a warning dialog   
#         if user:
#             espressops.unregister_user(user)
#             ui.navigate.to("/logout")

#     async def save_session_settings():
#         global save_lock, save_timer
#         if user:
#             with save_lock:
#                 if save_timer.is_alive():
#                     save_timer.cancel()
#                 save_timer=threading.Timer(SAVE_DELAY, function=espressops.update_preferences, args=(user, settings['search']))
#                 save_timer.start()
#     if user:
#         _render_user_profile(settings, user)
#         ui.separator()

#     ui.label('Preferences').classes("text-subtitle1")
#     # sequencing of bind_value and on_value_change is important.
#     # otherwise the value_change function will be called every time the page loads       
#     ui.select(
#         label="Topics of Interest", 
#         options=espressops.get_system_topic_id_label(), 
#         multiple=True,
#         with_input=True).bind_value(settings['search'], 'topics').on_value_change(save_session_settings).props("use-chips filled").classes("w-full")
    
#     if user:
#         ui.space()
#         ui.button("Delete Account", color="negative", on_click=delete_user).props("flat").classes("self-right").tooltip("Deletes your account, all connections and preferences")

def render_user_registration(settings: dict, temp_user: dict, success_func: Callable, failure_func: Callable):
    render_header()

    if not temp_user:
        ui.label("You really thought I wouldn't check for this?!")
        ui.button("My Bad!", on_click=failure_func)
        return
    
    with ui.stepper().props("vertical").classes("w-full") as stepper:
        with ui.step("You Look New! Let's Get You Signed-up.") :
            ui.label("User Agreement").classes("text-h6").tooltip("Kindly read the documents and agree to the terms to reduce our chances of going to jail.")
            ui.link("What is Espresso", "/docs/espresso.md", new_tab=True)
            ui.link("Terms of Use", "/docs/terms-of-use.md", new_tab=True)
            ui.link("Privacy Policy", "/docs/privacy-policy.md", new_tab=True)
            user_agreement = ui.checkbox(text="I have read and understood every single word in each of the links above. And I agree to selling to the terms and conditions.").tooltip("We are legally obligated to ask you this question.")
            with ui.stepper_navigation():
                ui.button("Done", color="primary", icon="thumb_up", on_click=lambda: success_func(espressops.register_user(temp_user))).bind_enabled_from(user_agreement, "value").props("unelevated")
                ui.button('Nope!', color="negative", icon="cancel", on_click=failure_func).props("outline")
        #     with ui.stepper_navigation():
        #         ui.button('Agreed', color="primary", on_click=stepper.next).props("unelevated").bind_enabled_from(user_agreement, "value")
        #         ui.button('Hell No!', color="negative", icon="cancel", on_click=failure_func).props("outline")
        # with ui.step("Tell Me Your Dreams") :
        #     ui.label("Personalization").classes("text-h6")
        #     with ui.row(wrap=False, align_items="center").classes("w-full"):
        #         temp_user[K_ID] = espressops.convert_new_userid(f"{temp_user['name']}@{temp_user[K_SOURCE]}")
        #         ui.input(label = "User ID").bind_value(temp_user, K_ID).props("outlined")
        #         _render_user_image(temp_user)        
        #     ui.label("Your Interests")      
        #     if temp_user['source'] == "reddit":     
        #         ui.button("Analyze From Reddit", on_click=lambda e: trigger_reddit_import(e.sender, temp_user['name'], [settings['search']['topics']])).classes("w-full")          
        #         ui.label("- or -").classes("text-caption self-center")                         
        #     ui.select(
        #         label="Topics", with_input=True, multiple=True, 
        #         options=espressops.get_system_topic_id_label()
        #     ).bind_value(settings['search'], 'topics').props("filled use-chips").classes("w-full").tooltip("We are saving this one too")

            

# def render_user_profile_update(settings: dict, user: dict):

#     render_banner_text("Update Your Profile")
#     with ui.card().classes("s-full"):
#         _render_user_profile(settings, user)

#     with ui.row().classes("w-full"):
#         ui.textarea(label="Your Preference Thoughts").props("outlined").classes("w-full").tooltip("Write your preference thoughts here")        
#         with ui.column().classes("w-full"):
#             ui.input(label="Search").props("outlined").classes("w-full").tooltip("Search for topics")
#             ui.button("Import From Reddit", icon=REDDIT_ICON_URL, on_click=lambda e: trigger_reddit_import(e.sender, user['name'], [settings['search']['topics']])).classes("w-full")
#             ui.button("Import From Medium", icon="import", on_click=lambda e: ui.notify(NOT_IMPLEMENTED)).classes("w-full")

#     with ui.row().classes("w-full"):
#         ui.select(
#             label="Select Topics to Follow", with_input=True, multiple=True, 
#             options=espressops.get_system_topic_id_label()
#         ).props("filled use-chips").classes("w-full").tooltip("Select your topics of interest")
        

#     ui.select(
#         label="Select Pages to Follow", with_input=True, multiple=True, 
#         options=espressops.get_system_topic_id_label()
#     ).props("filled use-chips").classes("w-full").tooltip("Select your topics of interest")

#     ui.select(
#         label="Select Users to Follow", with_input=True, multiple=True, 
#         options=espressops.get_system_topic_id_label()
#     ).props("filled use-chips").classes("w-full").tooltip("Select your topics of interest")
    
#     with ui.row().classes("w-full"):
#         ui.button("Update", color="primary").props("outline")
#         ui.button("Cancel", color="negative").props("outline")
    
# async def trigger_reddit_import(sender: ui.element, username: str, update_panels):
#     async def extract_topics():
#         text = redditor.collect_user_as_text(username, limit=10)     
#         if len(text) >= 100:                       
#             return espressops.match_categories(text)
        
#     with disable_button(sender):    
#         sender.props(":loading=true")  
#         new_topics = await extract_topics()
#         if not new_topics:
#             ui.notify(NO_INTERESTS_MESSAGE)
#             return            
#         for panel in update_panels:
#             panel = new_topics
#         sender.props(":loading=false")

# def _render_user_image(user):  
#     if user and user.get(espressops.IMAGE_URL):
#         ui.image(user.get(espressops.IMAGE_URL))

# def render_login_failed(success_forward, failure_forward):
#     with render_header():
#         ui.label(APP_NAME).classes("text-bold")

#     ui.label("Welp! That didn't work").classes("self-center")
#     with ui.row(align_items="stretch").classes("w-full").style("justify-content: center;"):
#         ui.button('Try Again', icon="login", on_click=lambda: ui.navigate.to(success_forward))
#         ui.button('Forget it', icon="cancel", color="negative", on_click=lambda: ui.navigate.to(failure_forward))

# TRENDING_TABS = [
#     {
#         "name": "articles", 
#         "label": "📰 News & Articles",
#         "kinds": [NEWS, BLOG]
#     },
#     {
#         "name": "posts", 
#         "label": "🗣️ Social Media",
#         "kinds": [POST, COMMENT]
#     }
# ]
