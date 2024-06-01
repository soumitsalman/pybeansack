import re
from . import renderer, slack_stores
from shared import config, messages
from icecream import ic
from slack_bolt import App
from slack_bolt.oauth.oauth_settings import OAuthSettings

_SLACK_SCOPES = ["app_mentions:read", "channels:history", "channels:read", "chat:write", "commands", "groups:history", "groups:read", "groups:write", "im:history", "im:read"]
# _POSTS_AND_ARTICLES = [renderer._ARTICLE, renderer._POST]

oauth_settings = OAuthSettings(
    client_id=config.get_slack_client_id(),
    client_secret=config.get_slack_client_secret(),
    scopes=_SLACK_SCOPES,
    installation_store=slack_stores.MongoInstallationStore(conn_str=config.get_db_connection_string(), app_name="espresso"),
    state_store=slack_stores.MongoOauthStateStore(conn_str=config.get_db_connection_string(), app_name="espresso", expiration_seconds=600)
)

# set up the initial app
slack_router = App(
    # token=config.get_slack_bot_token(),
    signing_secret=config.get_slack_signing_secret(),
    oauth_settings=oauth_settings
)

# @app.event("app_mention")
# def receive_mention(event, say, client):
#     receiver.new_message(
#         message_or_event = event,        
#         say=say, 
#         client=client
#     )



channel_mgr = renderer.ChannelManager()      

@slack_router.event("app_home_opened")
def update_home_tab(event, client):
    if event['tab'] == "home":        
        _refresh_home_tab(event['user'], client)

@slack_router.message()
def receive_message(message, say, client): 
    ic(message)
    say("I do nothing")

@slack_router.command("/trending")
def receive_trending(ack, command, client, say):
    ack()    
    channel_mgr.publish(
        client = client, 
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'],
        blocks = renderer.get_trending_items(username=command['user_id'], params=command['text'].split(' ')))

@slack_router.command("/more")
def receive_more(ack, command, client, say):
    ack()
    channel_mgr.next_page(
        client = client,
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'])
    
@slack_router.command("/lookfor")
def receive_lookfor(ack, command, client, say):
    ack()    
    say(messages.PROCESSING) 
    channel_mgr.publish(
        client = client, 
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'],
        blocks = renderer.get_beans_by_search(username=command['user_id'], search_text=command['text']))

@slack_router.command("/digest")
def receive_trending(ack, command, client, say):
    ack()   
    say(messages.PROCESSING) 
    channel_mgr.publish(
        client = client, 
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'],
        blocks = renderer.get_digests(username=command['user_id'], search_text=command['text']))

@slack_router.action(re.compile("^nugget//*"))
def receive_nugget_search(ack, action, client):
    ack()
    vals = action['value'].split("//")
    keyphrase, description, user_id, window = vals[0], vals[1], vals[2], int(vals[3])

    channel_mgr.publish(
        client = client, 
        user_id=user_id,
        blocks=renderer.get_beans_by_nugget(
            username=user_id, 
            keyphrase=keyphrase, 
            description=description, 
            show_by_preference=("show_by_preference" in action['action_id'].split("//")),
            window=window))

@slack_router.action(re.compile("^category//*"))
def receive_category_search(ack, action, client):
    ack()
    vals = action['value'].split("//")
    channel_mgr.publish(
        client = client, 
        user_id=vals[1],
        blocks = renderer.get_beans_by_category(username=vals[1], category=vals[0]))


@slack_router.action(re.compile("^update_interests:*"))
def trigger_update_interest(ack, action, body, client):
    ack()
    client.views_open(
        trigger_id=body['trigger_id'],
        view = renderer.UPDATE_INTEREST_VIEW
    )

@slack_router.view("new_interest_input")
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
            "blocks": renderer.get_user_home(user_id)
        }
    )
