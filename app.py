from env import *
import logging
from azure.monitor.opentelemetry import configure_azure_monitor

configure_azure_monitor(
    connection_string=APPINSIGHTS_CONNECTION_STRING, 
    logger_name=APP_NAME, 
    instrumentation_options={"fastapi": {"enabled": True}})  

import jwt
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, Query
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, FileResponse
from authlib.integrations.starlette_client import OAuth
from nicegui import ui, app
from pybeansack.embedding import *
from shared import beanops, espressops
from web_ui import vanilla
from shared.datamodel import *

JWT_TOKEN_KEY = 'espressotoken'
JWT_TOKEN_LIFETIME = timedelta(days=7) # TODO: change this later to 30 days
JWT_TOKEN_REFRESH_WINDOW = timedelta(hours=1) # TODO: change this later to 5 minutes

jwt_token_exp = lambda: datetime.now() + JWT_TOKEN_LIFETIME
jwt_token_needs_refresh = lambda data: (datetime.now() - JWT_TOKEN_REFRESH_WINDOW).timestamp() < data['exp']

logger: logging.Logger = logging.getLogger(APP_NAME)
oauth = OAuth()

def log(function: str, user: str|espressops.User = None, **kwargs):
    kwargs["user_id"] = (user.email if isinstance(user, espressops.User) else user) if user else app.storage.browser.get("user_id")
    kwargs = {key: ("|".join(value) if isinstance(value, list) else value) for key, value in kwargs.items() if value}
    logger.info(function, extra=kwargs)

def create_jwt_token(email: str):
    data = {
        "email": email,
        "iat": datetime.now(),
        "exp": jwt_token_exp()
    }
    return jwt.encode(data, APP_STORAGE_SECRET, algorithm="HS256")

def decode_jwt_token(token: str):
    try:
        data = jwt.decode(token, APP_STORAGE_SECRET, algorithms=["HS256"], verify=True)
        return data if (data and "email" in data) else None
    except Exception as err:
        log("JWT token decode error", None, error=str(err))
        return None

@app.on_startup
def initialize_server():
    global oauth, logger

    logger.setLevel(logging.INFO)
    
    embedder = ic(RemoteEmbeddings(LLM_BASE_URL, LLM_API_KEY, EMBEDDER_MODEL, EMBEDDER_N_CTX) \
        if LLM_BASE_URL else \
        BeansackEmbeddings(EMBEDDER_MODEL, EMBEDDER_N_CTX))
    beanops.initiatize(DB_CONNECTION_STRING, embedder)
    espressops.initialize(DB_CONNECTION_STRING, SB_CONNECTION_STRING, embedder)

    oauth.register(
        "google",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,        
        server_metadata_url=GOOGLE_SERVER_METADATA_URL,
        authorize_url=GOOGLE_AUTHORIZE_URL,
        access_token_url=GOOGLE_ACCESS_TOKEN_URL,
        api_base_url=GOOGLE_API_BASE_URL,
        userinfo_endpoint=GOOGLE_USERINFO_ENDPOINT,
        client_kwargs=GOOGLE_OAUTH_SCOPE,
        user_agent=APP_NAME
    )    
    oauth.register(
        "slack",
        client_id=SLACK_CLIENT_ID,
        client_secret=SLACK_CLIENT_SECRET,
        server_metadata_url=SLACK_SERVER_METADATA_URL,
        authorize_url=SLACK_AUTHORIZE_URL,
        access_token_url=SLACK_ACCESS_TOKEN_URL,
        api_base_url=SLACK_API_BASE_URL,
        client_kwargs=SLACK_OAUTH_SCOPE,
        user_agent=APP_NAME
    )
    oauth.register(
        "linkedin",
        client_id=LINKEDIN_CLIENT_ID,
        client_secret=LINKEDIN_CLIENT_SECRET,
        authorize_url=LINKEDIN_AUTHORIZE_URL,
        access_token_url=LINKEDIN_ACCESS_TOKEN_URL,
        api_base_url=LINKEDIN_API_BASE_URL,
        client_kwargs=LINKEDIN_OAUTH_SCOPE,
        user_agent=APP_NAME
    )
    oauth.register(
        name="reddit",
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        authorize_url=REDDIT_AUTHORIZE_URL,
        access_token_url=REDDIT_ACCESS_TOKEN_URL, 
        api_base_url=REDDIT_API_BASE_URL,
        client_kwargs=REDDIT_OAUTH_SCOPE,
        user_agent=APP_NAME
    )    
    
    logger.info("Web UI server initialized")

def extract_barista(barista_id: str) -> Barista:
    barista_id = barista_id.lower()
    barista = espressops.db.get_barista(barista_id)
    if not barista:
        raise HTTPException(status_code=404, detail=f"{barista_id} does not exist")
    return barista

def validate_doc(doc_id: str):
    if not bool(os.path.exists(f"docs/{doc_id}")):
        raise HTTPException(status_code=404, detail=f"{doc_id} does not exist")
    return doc_id

def validate_image(image_id: str):
    if not bool(os.path.exists(f"images/{image_id}")):
        raise HTTPException(status_code=404, detail=f"{image_id} does not exist")
    return image_id

def extract_user():
    token = app.storage.browser.get(JWT_TOKEN_KEY)
    if not token:
        return None
    data = decode_jwt_token(token)
    if not data:
        del app.storage.browser[JWT_TOKEN_KEY]
        return None
    user = espressops.db.get_user(data["email"])
    if not user:
        del app.storage.browser[JWT_TOKEN_KEY]
        return None
    # TODO: refresh token if close to expiration
    # if jwt_token_needs_refresh(data):
    #     print("refreshing token")
    #     app.storage.browser[JWT_TOKEN_KEY] = create_jwt_token(data["email"])
    return user

REGISTRATION_INFO_KEY = "registration_info"

def login_user(email: str):
    app.storage.browser[JWT_TOKEN_KEY] = ic(create_jwt_token(email))
    log("login_user", email)

def process_oauth_result(result: dict):
    existing_user = espressops.db.get_user(result['userinfo']['email'], result['userinfo']['iss'])
    if existing_user:
        login_user(existing_user.email)        
        return RedirectResponse("/")
    else:
        login_user(result['userinfo']['email'])
        app.storage.user[REGISTRATION_INFO_KEY] = result['userinfo']
        return RedirectResponse("/user/register")

def extract_registration_info():
    val = app.storage.user.get(REGISTRATION_INFO_KEY)
    if not val:
        raise HTTPException(status_code=401, detail="Unauthorized")
    del app.storage.user[REGISTRATION_INFO_KEY]
    return val

@app.get("/oauth/google/login")
async def google_oauth_login(request: Request):
    log("oauth_login", None, provider="google")
    return await oauth.google.authorize_redirect(request, os.getenv("BASE_URL") + "/oauth/google/redirect")

@app.get("/oauth/google/redirect")
async def google_oauth_redirect(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        return process_oauth_result(token)      
    except Exception as err:
        log("oauth_error", None, provider="google", error=str(err))
        return RedirectResponse("/")

@app.get("/oauth/slack/login")
async def slack_oauth_login(request: Request):
    log("oauth_login", None, provider="slack")
    return await oauth.slack.authorize_redirect(request, os.getenv("BASE_URL") + "/oauth/slack/redirect")

@app.get("/oauth/slack/redirect")
async def slack_oauth_redirect(request: Request):
    try:
        token = await oauth.slack.authorize_access_token(request)
        return process_oauth_result(token)  
    except Exception as err:
        log("oauth_error", None, provider="slack", error=str(err))
        return RedirectResponse("/")
    
@app.get("/oauth/linkedin/login")
async def linkedin_oauth_login(request: Request):
    log("oauth_login", None, provider="linkedin")
    return await oauth.linkedin.authorize_redirect(request, os.getenv("BASE_URL") + "/oauth/linkedin/redirect")

@app.get("/oauth/linkedin/redirect")
async def linkedin_oauth_redirect(request: Request):
    try:
        token = await oauth.linkedin.authorize_access_token(request)
        return process_oauth_result(token) 
    except Exception as err:
        log("oauth_error", None, provider="linkedin", error=str(err))
        return RedirectResponse("/")

@app.get("/user/me/logout")
async def logout_user(user: espressops.User = Depends(extract_user)):
    log("logout_user", user)
    if JWT_TOKEN_KEY in app.storage.browser:
        del app.storage.browser[JWT_TOKEN_KEY]
    return RedirectResponse("/")

@app.get("/user/me/delete")
async def delete_user(user: espressops.User = Depends(extract_user)):
    log("delete_user", user)
    espressops.db.delete_user(user.email)
    if JWT_TOKEN_KEY in app.storage.browser:
        del app.storage.browser[JWT_TOKEN_KEY]
    return RedirectResponse("/")

@ui.page("/")
async def home(user: espressops.User = Depends(extract_user)):  
    log('home', user)
    await vanilla.render_trending_snapshot(user)

@ui.page("/beans")
async def beans(
    user: espressops.User = Depends(extract_user),
    tag: list[str] | None = Query(max_length=beanops.MAX_LIMIT, default=None),
    kind: str | None = Query(default=None)
):
    log('beans', user, tag=tag, kind=kind)
    if tag:
        await vanilla.render_tags_page(user, tag, kind)
    else:
        await vanilla.render_trending_snapshot(user)

@ui.page("/barista/{barista_id}")
async def barista(
    user: User = Depends(extract_user),
    barista: Barista = Depends(extract_barista, use_cache=True)
): 
    log('barista', user, page_id=barista.id) 
    await vanilla.render_barista_page(user, barista)

@ui.page("/search")
async def search(
    user: espressops.User = Depends(extract_user),
    q: str = None,
    acc: float = Query(ge=0, le=1, default=beanops.DEFAULT_ACCURACY),
    tag: list[str] | None = Query(max_length=beanops.MAX_LIMIT, default=None),
    kind: str | None = Query(default=None),
    ndays: int = Query(ge=beanops.MIN_WINDOW, le=beanops.MAX_WINDOW, default=beanops.DEFAULT_WINDOW)
):
    log('search', user, q=q, acc=acc, ndays=ndays)
    await vanilla.render_search(user, q, acc)

@ui.page("/user/register", title="Registration")
async def register_user(userinfo: dict = Depends(extract_registration_info)):
    log('register_user', userinfo)
    await vanilla.render_registration(userinfo)

@ui.page("/docs/{doc_id}")
async def document(
    user: espressops.User = Depends(extract_user),
    doc_id: str = Depends(validate_doc)
):
    log('docs', user, page_id=doc_id)
    await vanilla.render_doc(user, doc_id)  
    
@app.get("/images/{image_id}")
async def image(image_id: str = Depends(validate_image)):    
    return FileResponse(f"./images/{image_id}", media_type="image/png")
    
GOOGLE_ANALYTICS_SCRIPT = '''
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-NBSTNYWPG1"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-NBSTNYWPG1');
</script>
'''

def run():
    app.add_middleware(SessionMiddleware, secret_key=APP_STORAGE_SECRET) # needed for oauth
    ui.add_head_html(GOOGLE_ANALYTICS_SCRIPT, shared=True)
    ui.run(
        title=APP_NAME, 
        storage_secret=APP_STORAGE_SECRET,
        dark=True, 
        favicon="./images/favicon.ico", 
        port=8080, 
        show=False,
        uvicorn_reload_includes="*.py,*/web_ui/styles.css",
        uvicorn_logging_level="info"
    )

if __name__ in {"__main__", "__mp_main__"}:
    run()