# import env
# import logging
# from azure.monitor.opentelemetry import configure_azure_monitor

# configure_azure_monitor(
#     connection_string=env.az_insights_connection_string(), 
#     logger_name=env.app_name(), 
#     instrumentation_options={"fastapi": {"enabled": True}})  
# logger: logging.Logger = logging.getLogger(env.app_name())
# logger.setLevel(logging.INFO)

# from pybeansack.datamodels import *
# from pybeansack.embedding import *
# from shared import beanops, espressops, messages
# from shared.utils import *
# # from authlib.integrations.starlette_client import OAuth

# # oauth = OAuth()
# def initialize_server():
#     embedder = RemoteEmbeddings(env.llm_base_url(), env.llm_api_key(), env.embedder_model(), env.embedder_n_ctx()) \
#         if env.llm_base_url() else \
#         BeansackEmbeddings(env.embedder_model(), env.embedder_n_ctx())
#     beanops.initiatize(env.db_connection_str(), embedder)
#     espressops.initialize(env.db_connection_str(), env.sb_connection_str(), embedder)

#     # oauth.register(
#     #     name=REDDIT,
#     #     client_id=env.reddit_client_id(),
#     #     client_secret=env.reddit_client_secret(),
#     #     user_agent=env.app_name(),
#     #     authorize_url='https://www.reddit.com/api/v1/authorize',
#     #     access_token_url='https://www.reddit.com/api/v1/access_token', 
#     #     api_base_url="https://oauth.reddit.com/",
#     #     client_kwargs={'scope': 'identity mysubreddits'}
#     # )
#     # oauth.register(
#     #     name=SLACK,
#     #     client_id=env.slack_client_id(),
#     #     client_secret=env.slack_client_secret(),
#     #     user_agent=env.app_name(),
#     #     authorize_url='https://slack.com/oauth/authorize',
#     #     access_token_url='https://slack.com/api/oauth.access',
#     #     client_kwargs={'scope': 'identity.basic,identity.avatar'},
#     # )



# # ##### SLACK APP SECTION #####
# # from slack_bolt.adapter.fastapi import SlackRequestHandler
# # from slack_ui.handler import slack_app

# # handler = SlackRequestHandler(slack_app)

# # @app.post("/slack/events")
# # @app.post("/slack/commands")
# # @app.post("/slack/actions")
# # @app.get("/slack/oauth-redirect")
# # @app.get("/slack/install")
# # async def receive_slack_app_events(req: Request):
# #     res = await handler.handle(req)
# #     return res

# ##### WEB APP SECTION #####
# # from fastapi import Query, Depends
# # from starlette.requests import Request
# # from starlette.responses import RedirectResponse
# from nicegui import app, ui
# from fastapi import Query, Depends
# # from fastapi.responses import FileResponse, Response
# from icecream import ic
# import web_ui.vanilla
# import web_ui.renderer

# def session_settings() -> dict:
#     if 'settings' not in app.storage.user:
#         app.storage.user['settings'] = {}
#     return app.storage.user['settings']

# def last_page() -> str:
#     return session_settings().get('last_page', "/")

# def temp_user():
#     return app.storage.user.get("temp_user")

# def set_temp_user(user):
#     app.storage.user["temp_user"] = user

# def clear_temp_user():
#     if 'temp_user' in app.storage.user:
#         del app.storage.user["temp_user"]

# def logged_in_user():
#     return app.storage.user.get('logged_in_user')

# # def set_logged_in_user(registered_user):
# #     app.storage.user['logged_in_user'] = registered_user  
# #     settings = session_settings() 
# #     if espressops.PREFERENCES in registered_user:        
# #         settings['search']['last_ndays'] = registered_user[espressops.PREFERENCES]['last_ndays']
# #     settings['search']['topics'] = espressops.get_user_category_ids(registered_user) or settings['search']['topics']

# # def log_out_user():
# #     if 'logged_in_user' in app.storage.user:
# #         del app.storage.user['logged_in_user']

# # @app.get("/web/slack/login")
# # async def slack_login(request: Request):
# #     redirect_uri = env.base_url()+"/web/slack/oauth-redirect"
# #     return await oauth.slack.authorize_redirect(request, redirect_uri)

# # @app.get("/web/slack/oauth-redirect")
# # async def slack_web_redirect(request: Request):
# #     try:
# #         token = await oauth.slack.authorize_access_token(request)
# #         user = (await oauth.slack.get('https://slack.com/api/users.identity', token=token)).json()    
# #         return _redirect_after_auth(user['user']['name'], user['user']['id'], user['user'].get('image_72'), SLACK, token)
# #     except Exception as err:
# #         logging.warning(err)
# #         return RedirectResponse("/login-failed?source=slack")

# # @app.get("/reddit/login")
# # async def reddit_login(request: Request):
# #     redirect_uri = env.base_url()+"/reddit/oauth-redirect"
# #     return await oauth.reddit.authorize_redirect(request, redirect_uri)

# # @app.get("/reddit/oauth-redirect")
# # async def reddit_redirect(request: Request):    
# #     try:
# #         token = await oauth.reddit.authorize_access_token(request)
# #         user = (await oauth.reddit.get('https://oauth.reddit.com/api/v1/me', token=token)).json()
# #         return _redirect_after_auth(user['name'], user['id'], user.get('icon_img'), REDDIT, token)
# #     except Exception as err:
# #         logging.warning(err)
# #         return RedirectResponse("/login-failed?source=reddit")

# # def _redirect_after_auth(name, id, image_url, source, token):
# #     authenticated_user = ic({
# #         espressops.NAME: name,
# #         espressops.SOURCE_ID: id,
# #         espressops.SOURCE: source,
# #         espressops.IMAGE_URL: image_url
# #     })
# #     # if a user is already logged in then add this as a connection
# #     current_user = ic(logged_in_user())
# #     if current_user:
# #         espressops.add_connection(current_user, authenticated_user)
# #         current_user[espressops.CONNECTIONS][source]=name
# #         log(logger, 'connection added')
# #         return RedirectResponse(last_page())
        
# #     # if no user is logged in but there is an registered user with this cred then log-in that user    
# #     registered_user = ic(espressops.get_user(authenticated_user))
# #     if registered_user:
# #         set_logged_in_user(registered_user)
# #         log(logger, 'logged in')
# #         return RedirectResponse(last_page()) 

# #     set_temp_user(authenticated_user)
# #     return RedirectResponse("/user-registration")

# # @app.get('/logout')
# # def logout():
# #     log(logger, 'logged out')
# #     log_out_user()
# #     return RedirectResponse(last_page())

# # @app.get("/images/{name}")
# # async def image(name: str):
# #     path = "./images/"+name
# #     if os.path.exists(path):
# #         return FileResponse(path, media_type="image/png")
# #     return Response(content=messages.RESOURCE_NOT_FOUND, status_code=404)

# @ui.page('/login-failed')
# async def login_failed(source: str):
#     web_ui.vanilla.render_login_failed(f'/{source}/login', last_page())

# @ui.page('/user-registration')
# def user_registration():
#     log(logger, 'user_registration', user_id=temp_user()[espressops.NAME] if temp_user() else None)
#     web_ui.vanilla.render_user_registration(
#         session_settings(), 
#         temp_user(),
#         lambda user: [set_logged_in_user(user), clear_temp_user(), ui.navigate.to(last_page())],
#         lambda: [clear_temp_user(), ui.navigate.to(last_page())])

# @ui.page("/")
# async def home():  
#     settings = session_settings()
#     settings['last_page'] = "/" 
#     log(logger, 'home')
#     await web_ui.vanilla.render_home(settings, logged_in_user())

# @ui.page("/search")
# async def search(
#     q: str = None, 
#     acc: float = Query(ge=0, le=1, default=DEFAULT_ACCURACY),
#     tag: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
#     kind: list[str] | None = Query(max_length=MAX_LIMIT, default=None),
#     ndays: int | None = Query(ge=MIN_WINDOW, le=MAX_WINDOW, default=MIN_WINDOW)):

#     settings = session_settings()
#     settings['last_page'] = web_ui.renderer.create_navigation_target("/search", q=q, acc=acc, tag=tag, kind=kind, ndays=ndays)
#     log(logger, 'search', q=q, tag=tag, kind=kind, ndays=ndays, acc=acc)
#     await web_ui.vanilla.render_search(settings, logged_in_user(), q, acc, tag, kind, ndays)

# @ui.page("/channel/{channel_id}")
# async def channel(channel_id: str = Depends(lambda channel_id: channel_id if bool(espressops.get_channel(channel_id)) else None, use_cache=True)):    
#     settings = session_settings()
#     settings['last_page'] = web_ui.renderer.create_navigation_target(f"/channel/{channel_id}") 
#     log(logger, 'channel', page_id=channel_id)
#     await web_ui.vanilla.render_channel(settings, logged_in_user(), channel_id)

# @ui.page("/docs/{doc_id}")
# async def document(doc_id: str = Depends(lambda doc_id: doc_id if bool(os.path.exists(f"./documents/{doc_id}.md")) else None, use_cache=True)):
#     log(logger, 'docs', page_id=doc_id)
#     await web_ui.vanilla.render_doc(session_settings(), logged_in_user(), doc_id)      

# initialize_server()
# # ui.run(title=env.app_name(), favicon="images/favicon.jpg", storage_secret=env.internal_auth_token(), host="0.0.0.0", port=8080, show=False, binding_refresh_interval=0.3, dark=True)
# ui.run(title=env.app_name())