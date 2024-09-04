import re
from . import renderer, slack_stores
from shared.config import *
from shared.messages import *
from shared import prompt_parser, beanops, espressops
from icecream import ic
from slack_bolt import App
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.state_store import FileOAuthStateStore

class UserSettings:
    topics: list[str]
    last_ndays: int
    connections: dict
    
    def __init__(self, topics, last_ndays, connections):
        self.topics = topics
        self.last_ndays = last_ndays
        self.connections = connections

SCOPES = ["app_mentions:read", "channels:history", "channels:read", "chat:write", "commands", "groups:history", "groups:read", "groups:write", "im:history", "im:read"]
# set up the initial app
slack_app = App(
    token=slack_bot_token(),
    signing_secret=slack_signing_secret(),
    oauth_settings=OAuthSettings(
        client_id=slack_client_id(),
        client_secret=slack_client_secret(),
        scopes=SCOPES,
        installation_store=slack_stores.MongoInstallationStore(conn_str=db_connection_str(), app_name="espresso"),
        state_store=FileOAuthStateStore(client_id=slack_client_id(), base_dir=".slack", expiration_seconds=600),
        redirect_uri_path="/slack/oauth-redirect"
    )
) 
channel_mgr = renderer.ChannelManager() 
sessions = dict()

def session_settings(userid):
    if userid not in sessions:        
        registered_user = espressops.get_user({espressops.SOURCE_ID: userid, espressops.SOURCE: SLACK})
        if registered_user:
            sessions[userid] = {
                "topics": espressops.get_topics(registered_user),
                "last_ndays": registered_user[espressops.PREFERENCES]['last_ndays'],
                "connections": registered_user[espressops.CONNECTIONS]}
        else:
            sessions[userid] = {
                "topics": espressops.get_system_topics(),
                "last_ndays": DEFAULT_WINDOW,
                "connections": {}}
        
    return sessions[userid]
        

@slack_app.event("app_home_opened")
def update_home_tab(event, client):
    if event['tab'] == "home":
        client.views_publish(
            user_id = event['user'],
            view = {
                "type": "home",
                "blocks": renderer.render_home(session_settings(event['user']))
            }
        ) 

@slack_app.message()
def receive_message(message, say, client): 
    _process_prompt(message['text'], message['user'], say, message['channel'], message['channel_type'], client)

@slack_app.command("/espresso")
def receive_command(ack, command, say, client):
    ack()  
    _process_prompt(command['text'], command['user_id'], say, command['channel_id'], command['channel_name'], client)

def _process_prompt(prompt, userid, say, channel_id, channel_type, client):
    result = prompt_parser.parser.parse(prompt, session_settings(userid))
    beans, count, more_beans = None, None, None
    say("Processing")

    if not result.task:
        beans = beanops.search(query=ic(result.prompt), categories=None, tags=None, kinds=None, last_ndays=None, start_index=0, topn=MAX_LIMIT)
    if result.task in ["lookfor", "search"]:
        beans = beanops.search(query=result.query, categories=result.category, tags=result.keyword, kinds=result.kind, last_ndays=result.last_ndays, start_index=0, topn=MAX_LIMIT)        
    if result.task == "trending":
        beans = beanops.trending(query=ic(result.query), categories=ic(result.category), tags=result.keyword, kinds=result.kind, last_ndays=result.last_ndays, start_index=0, topn=MAX_LIMIT)
    if result.task == "publish":
        # TODO: do something
        pass
    
    say(f"{len(beans or [])} beans found")

    # if beans:
    #     channel_mgr.publish(
    #         client=client, 
    #         channel_id=message['channel'],
    #         channel_type=message['channel_type'], 
    #         user_id=message['user'],
    #         blocks=renderer.render_beans(beans),
    #         count = 10,
    #         beans_iter = lambda: None)

@slack_app.action(re.compile("^category:*"))
def receive_category_search(ack, action, client):
    ack()
    vals = ic(action)['value'].split("//")
    channel_mgr.publish(
        client = client, 
        user_id=vals[1],
        blocks = renderer.get_beans_by_category(username=vals[1], category=vals[0]))
    
@slack_app.action(re.compile("^nugget//*"))
def receive_keyword_search(ack, action, event, client):
    ack()
    ic(event)
    vals = ic(action)['value'].split("//")
    channel_mgr.publish(
        client = client, 
        user_id=vals[1],
        blocks = renderer.get_beans_by_category(username=vals[1], category=vals[0]))

    


# @slack_app.command("/trending")
# def receive_trending(ack, command, client, say):
#     ack()    
#     channel_mgr.publish(
#         client = client, 
#         channel_id = command['channel_id'],
#         channel_type=command['channel_name'], 
#         user_id=command['user_id'],
#         blocks = renderer.get_trending_items(username=command['user_id'], params=command['text'].split(' ')))

# @slack_app.command("/more")
# def receive_more(ack, command, client, say):
#     ack()
#     channel_mgr.next_page(
#         client = client,
#         channel_id = command['channel_id'],
#         channel_type=command['channel_name'], 
#         user_id=command['user_id'])
    
# @slack_app.command("/lookfor")
# def receive_lookfor(ack, command, client, say):
#     ack()    
#     say(messages.PROCESSING) 
#     channel_mgr.publish(
#         client = client, 
#         channel_id = command['channel_id'],
#         channel_type=command['channel_name'], 
#         user_id=command['user_id'],
#         blocks = renderer.get_beans_by_search(username=command['user_id'], search_text=command['text']))

# @slack_app.command("/digest")
# def receive_trending(ack, command, client, say):
#     ack()   
#     say(messages.PROCESSING) 
#     channel_mgr.publish(
#         client = client, 
#         channel_id = command['channel_id'],
#         channel_type=command['channel_name'], 
#         user_id=command['user_id'],
#         blocks = renderer.get_digests(username=command['user_id'], search_text=command['text']))

# @slack_app.action(re.compile("^nugget//*"))
# def receive_nugget_search(ack, action, client):
#     ack()
#     vals = action['value'].split("//")
#     keyphrase, description, user_id, window = vals[0], vals[1], vals[2], int(vals[3])

#     channel_mgr.publish(
#         client = client, 
#         user_id=user_id,
#         blocks=renderer.get_beans_by_nugget(
#             username=user_id, 
#             keyphrase=keyphrase, 
#             description=description, 
#             show_by_preference=("show_by_preference" in action['action_id'].split("//")),
#             window=window))




@slack_app.action(re.compile("^update_interests:*"))
def trigger_update_interest(ack, action, body, client):
    ack()
    client.views_open(
        trigger_id=body['trigger_id'],
        view = renderer.UPDATE_INTEREST_VIEW
    )

@slack_app.view("new_interest_input")
def new_interests(ack, body, view, client):
    ack()
    user_id = body["user"]["id"]
    interests = view["state"]["values"]["new_interest_input"]["new_interests"]['value']
    # update database
    renderer.update_user_preferences(user_id=user_id, interests=[item.strip().lower() for item in interests.split(',') if item.strip()])
    # update home view
    _refresh_home_tab(user_id, client)

def _refresh_home_tab(user_id, client):
    client.views_publish(
        user_id = user_id,
        view = {
            "type": "home",
            "blocks": renderer.render_home(user_id)
        }
    )
