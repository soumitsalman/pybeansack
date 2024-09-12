from itertools import chain
import re
from queue import Queue
from .renderer import *
from .slack_stores import MongoInstallationStore
from shared.config import *
from shared.messages import *
from shared import prompt_parser, beanops, espressops
from icecream import ic
from slack_bolt import App
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.state_store import FileOAuthStateStore

LOCAL_MAX_LIMIT = 20
LOCAL_MAX_ITEMS_PER_PAGE = 3

SCOPES = ["app_mentions:read", "channels:history", "channels:read", "chat:write", "commands", "groups:history", "groups:read", "groups:write", "im:history", "im:read"]
# set up the initial app
slack_app = App(
    token=slack_bot_token(),
    signing_secret=slack_signing_secret(),
    oauth_settings=OAuthSettings(
        client_id=slack_client_id(),
        client_secret=slack_client_secret(),
        scopes=SCOPES,
        installation_store=MongoInstallationStore(conn_str=db_connection_str(), app_name="espresso"),
        state_store=FileOAuthStateStore(client_id=slack_client_id(), base_dir=".slack", expiration_seconds=600),
        redirect_uri_path="/slack/oauth-redirect"
    )
) 
sessions = dict()

def session_settings(userid):
    if userid not in sessions:        
        registered_user = espressops.get_user({espressops.SOURCE_ID: userid, espressops.SOURCE: SLACK})
        if registered_user:
            sessions[userid] = {
                "search": {
                    "topics": espressops.get_topics(registered_user),
                    "last_ndays": registered_user[espressops.PREFERENCES]['last_ndays']
                },
                "user": registered_user
            }                
        else:
            sessions[userid] = {
                "search": {
                    "topics": DEFAULT_CATEGORIES,
                    "last_ndays": MIN_WINDOW
                }
            }        
    return sessions[userid]

@slack_app.event("app_home_opened")
def update_home_tab(event, client):
    if event['tab'] == "home":
        _render_home_tab(event['user'], client)

@slack_app.message()
def handle_message(message, say): 
    _process_prompt(message['text'], message['user'], say)

@slack_app.command("/espresso")
def handle_command(ack, command, say):
    ack()  
    _process_prompt(command['text'], command['user_id'], say)

def _process_prompt(prompt, userid, say):
    settings = session_settings(userid)
    beans, left, response = None, None, None
    result = prompt_parser.console_parser.parse(prompt, settings['search'])
    if not result.task:
        _new_message_queue(
            settings,
            beanops.search(
                query=result.query, tags=None, kinds=None, last_ndays=None, start_index=0, topn=LOCAL_MAX_LIMIT))   
        beans, left = _dequeue_message(settings)
    if result.task in ["lookfor", "search"]: 
        _new_message_queue(
            settings,
            beanops.search(
                query=result.query, tags=result.keyword, kinds=result.kind, last_ndays=result.last_ndays, start_index=0, topn=LOCAL_MAX_LIMIT))        
        beans, left = _dequeue_message(settings)
    if result.task in ["trending"]: 
        _new_message_queue(
            settings,
            beanops.trending(
                query=result.query, tags=result.keyword, kinds=result.kind, last_ndays=result.last_ndays, start_index=0, topn=LOCAL_MAX_LIMIT))        
        beans, left = _dequeue_message(settings)
    if result.task == "more":
        beans, left = _dequeue_message(settings)
    if result.task == "publish":
        response = NO_ACTION        

    _say_beans(beans, left, say, None)
    if response:
        say(response)

@slack_app.action(re.compile("^category:*"))
def handle_trending_in_category(ack, action, body, say):
    ack()
    settings = session_settings(body['user']['id'])
    _new_message_queue(
        settings,
        beanops.trending(None, action['value'], None, DEFAULT_KIND, MIN_WINDOW, 0, LOCAL_MAX_LIMIT))
    beans, left = _dequeue_message(settings)
    _say_beans(beans, left, say, body['user']['id'])
    
@slack_app.action(re.compile("^keyword:*"))
def handle_trending_in_keyword(ack, action, body, say):
    ack()
    settings = session_settings(body['user']['id'])
    _new_message_queue(
        settings,
        beanops.trending(None, None, action['value'], DEFAULT_KIND, MIN_WINDOW, 0, LOCAL_MAX_LIMIT))
    beans, left = _dequeue_message(settings)
    _say_beans(beans, left, say, body['user']['id'])
   
def _say_beans(beans, left, say, channel_id):
    if beans:
        say(
            blocks=list(chain(*(render_whole_bean(bean) for bean in beans))), 
            text=f"Showing {len(beans)} stories",
            channel=channel_id)
        
    response = (MORE_BEANS_LEFT if left else None) if beans else NOTHING_FOUND
    if response:
        say(response, channel=channel_id)

def _new_message_queue(settings, beans):
    if not beans:
        if "more" in settings:
            del settings['more']
        return
    
    queue = Queue(maxsize=MAX_LIMIT)
    for i in range(0, len(beans), LOCAL_MAX_ITEMS_PER_PAGE):
        queue.put_nowait(beans[i: i+LOCAL_MAX_ITEMS_PER_PAGE])
    settings["more"] = queue

def _dequeue_message(settings) -> tuple[list[Bean], bool]: 
    beans = None       
    if "more" in settings:
        if not settings['more'].empty():
            beans = settings['more'].get_nowait()
        if settings['more'].empty():
            del settings['more']
    return beans, "more" in settings

@slack_app.action(re.compile("^update:*"))
def trigger_update_preference(ack, action, body, say):
    ack()
    say(NO_ACTION, channel=body['user']['id'])

@slack_app.action(re.compile("^(delete-account|register-account):*"))
def update_account(ack, action, body, say):
    ack()
    say(NO_ACTION, channel=body['user']['id'])

# @slack_app.view("new_interest_input")
# def new_interests(ack, body, view, client):
#     ack()
#     user_id = body["user"]["id"]
#     interests = view["state"]["values"]["new_interest_input"]["new_interests"]['value']
#     # update database
#     update_user_preferences(user_id=user_id, interests=[item.strip().lower() for item in interests.split(',') if item.strip()])
#     # update home view
#     _render_home_tab(user_id, client)

def _render_home_tab(userid, client):
    client.views_publish(
        user_id = userid,
        view = {
            "type": "home",
            "blocks": render_home_blocks(session_settings(userid))
        }
    )
