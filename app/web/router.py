import logging
from azure.monitor.opentelemetry import configure_azure_monitor
from app.shared.env import *

if APPINSIGHTS_CONNECTION_STRING:   
    configure_azure_monitor(
        connection_string=APPINSIGHTS_CONNECTION_STRING, 
        logger_name=APP_NAME, 
        instrumentation_options={"fastapi": {"enabled": True}})  

from app.pybeansack.embedding import *
from app.shared import beanops, espressops
from app.web import vanilla
from app.shared.datamodel import *
from app.shared.utils import log

import jwt
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, Query
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, FileResponse
from authlib.integrations.starlette_client import OAuth
from nicegui import ui, app

JWT_TOKEN_KEY = 'espressotoken'
JWT_TOKEN_LIFETIME = timedelta(days=7) # TODO: change this later to 30 days
JWT_TOKEN_REFRESH_WINDOW = timedelta(hours=1) # TODO: change this later to 5 minutes

jwt_token_exp = lambda: datetime.now() + JWT_TOKEN_LIFETIME
jwt_token_needs_refresh = lambda data: (datetime.now() - JWT_TOKEN_REFRESH_WINDOW).timestamp() < data['exp']
user_id = lambda user: user.email if user else app.storage.browser.get("id")

logger: logging.Logger = logging.getLogger(APP_NAME)
oauth = OAuth()

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
        log("jwt_token_decode_error", user_id=app.storage.browser.get("id"), error=str(err))
        return None

@app.on_startup
def initialize_server():
    global oauth, logger

    logger.setLevel(logging.INFO)
    
    embedder = RemoteEmbeddings(LLM_BASE_URL, LLM_API_KEY, EMBEDDER_MODEL, EMBEDDER_N_CTX) \
        if LLM_BASE_URL else \
        BeansackEmbeddings(EMBEDDER_MODEL, EMBEDDER_N_CTX)
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
    
    log("server_initialized")

def validate_barista(barista_id: str) -> Barista:
    barista_id = barista_id.lower()
    barista = espressops.db.get_barista(barista_id)
    if not barista:
        raise HTTPException(status_code=404, detail=f"{barista_id} not found")
    
    if not barista.public:
        user = extract_user()
        if not user or barista.owner != user.email:
            raise HTTPException(status_code=401, detail="Unauthorized")
    return barista

def validate_doc(doc_id: str):
    if not bool(os.path.exists(f"docs/{doc_id}")):
        raise HTTPException(status_code=404, detail=f"{doc_id} not found")
    return f"./docs/{doc_id}"

def validate_image(image_id: str):
    if not bool(os.path.exists(f"images/{image_id}")):
        raise HTTPException(status_code=404, detail=f"{image_id} not found")
    return f"./images/{image_id}"

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

def logged_in_user():
    user = extract_user()
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user

REGISTRATION_INFO_KEY = "registration_info"

def login_user(user: dict|User):
    email = user.email if isinstance(user, User) else user['email']
    app.storage.browser[JWT_TOKEN_KEY] = create_jwt_token(email)
    log("login_user", user_id=email)

def process_oauth_result(result: dict):
    existing_user = espressops.db.get_user(result['userinfo']['email'], result['userinfo']['iss'])
    if existing_user:
        login_user(existing_user)        
        return RedirectResponse("/")
    else:
        login_user(result['userinfo'])
        app.storage.browser[REGISTRATION_INFO_KEY] = result['userinfo']
        return RedirectResponse("/user/register")

def extract_registration_info():
    val = app.storage.browser.get(REGISTRATION_INFO_KEY)
    if not val:
        raise HTTPException(status_code=401, detail="Unauthorized")
    del app.storage.browser[REGISTRATION_INFO_KEY]
    return val

@app.get("/oauth/google/login")
async def google_oauth_login(request: Request):
    log("oauth_login", user_id=app.storage.browser.get("id"), provider="google")
    return await oauth.google.authorize_redirect(request, os.getenv("BASE_URL") + "/oauth/google/redirect")

@app.get("/oauth/google/redirect")
async def google_oauth_redirect(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        return process_oauth_result(token)
    except Exception as err:
        log("oauth_error", user_id=app.storage.browser.get("id"), provider="google", error=str(ic(err)))
        return RedirectResponse("/")

@app.get("/oauth/slack/login")
async def slack_oauth_login(request: Request):
    log("oauth_login", user_id=app.storage.browser.get("id"), provider="slack")
    return await oauth.slack.authorize_redirect(request, os.getenv("BASE_URL") + "/oauth/slack/redirect")

@app.get("/oauth/slack/redirect")
async def slack_oauth_redirect(request: Request):
    try:
        token = await oauth.slack.authorize_access_token(request)
        return process_oauth_result(token)  
    except Exception as err:
        log("oauth_error", user_id=app.storage.browser.get("id"), provider="slack", error=str(ic(err)))
        return RedirectResponse("/")
    
@app.get("/oauth/linkedin/login")
async def linkedin_oauth_login(request: Request):
    log("oauth_login", user_id=app.storage.browser.get("id"), provider="linkedin")
    return await oauth.linkedin.authorize_redirect(request, os.getenv("BASE_URL") + "/oauth/linkedin/redirect")

@app.get("/oauth/linkedin/redirect")
async def linkedin_oauth_redirect(request: Request):
    try:
        token = await oauth.linkedin.authorize_access_token(request)
        return process_oauth_result(token) 
    except Exception as err:
        log("oauth_error", user_id=app.storage.browser.get("id"), provider="linkedin", error=str(err))
        return RedirectResponse("/")

@app.get("/user/me/logout")
async def logout_user(user: espressops.User = Depends(logged_in_user)):
    log("logout_user", user_id=user_id(user))
    if JWT_TOKEN_KEY in app.storage.browser:
        del app.storage.browser[JWT_TOKEN_KEY]
    return RedirectResponse("/")

@app.get("/user/me/delete")
async def delete_user(user: espressops.User = Depends(logged_in_user)):
    log("delete_user", user_id=user_id(user))
    espressops.db.delete_user(user.email)
    if JWT_TOKEN_KEY in app.storage.browser:
        del app.storage.browser[JWT_TOKEN_KEY]
    return RedirectResponse("/")

@app.get("/docs/{doc_id}")
async def document(
    user: espressops.User = Depends(extract_user),
    doc_id: str = Depends(validate_doc, use_cache=True)
):
    log('docs', user_id=user_id(user), page_id=doc_id)
    return FileResponse(doc_id, media_type="text/markdown")
    
@app.get("/images/{image_id}")
async def image(image_id: str = Depends(validate_image, use_cache=True)):    
    return FileResponse(image_id, media_type="image/png")

@ui.page("/", title="Espresso")
async def home(user: espressops.User = Depends(extract_user)):  
    log('home', user_id=user_id(user))
    await vanilla.render_home(user)

@ui.page("/beans", title="Espresso Beans")
async def beans(
    user: espressops.User = Depends(extract_user),
    tag: list[str] | None = Query(max_length=beanops.MAX_LIMIT, default=None),
    kind: str | None = Query(default=None)
):
    log('beans', user_id=user_id(user), tag=tag, kind=kind)
    await vanilla.render_beans_page(user, tag, kind)

@ui.page("/baristas", title="Espresso Shots")
async def snapshot(user: User = Depends(extract_user)): 
    log('baristas', user_id=user_id(user)) 
    await vanilla.render_trending_snapshot(user)

@ui.page("/baristas/{barista_id}", title="Espresso")
async def barista(
    user: User = Depends(extract_user),
    barista: Barista = Depends(validate_barista, use_cache=True)
): 
    log('baristas', user_id=user_id(user), page_id=barista.id) 
    await vanilla.render_barista_page(user, barista)

@ui.page("/search", title="Espresso Search")
async def search(
    user: espressops.User = Depends(extract_user),
    q: str = None,
    acc: float = Query(ge=0, le=1, default=beanops.DEFAULT_ACCURACY),
    tag: list[str] | None = Query(max_length=beanops.MAX_LIMIT, default=None),
    kind: str | None = Query(default=None),
    ndays: int = Query(ge=beanops.MIN_WINDOW, le=beanops.MAX_WINDOW, default=beanops.DEFAULT_WINDOW)
):
    log('search', user_id=user_id(user), q=q, acc=acc, ndays=ndays)
    await vanilla.render_search(user, q, acc)

@ui.page("/user/register", title="Espresso: User Registration")
async def register_user(userinfo: dict = Depends(extract_registration_info)):
    log('register_user', user_id=userinfo['email'])
    await vanilla.render_registration(userinfo)
    
GOOGLE_ANALYTICS_SCRIPT = '''
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-NBSTNYWPG1"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-NBSTNYWPG1');
</script>
<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
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
        uvicorn_reload_includes="*.py,*/web/styles.css"
    )