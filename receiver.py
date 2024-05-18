import re
import renderer
import config
from icecream import ic
from slack_bolt import App
from slack_bolt.oauth.oauth_settings import OAuthSettings
import slack_stores
import queue
from itertools import chain

_SLACK_SCOPES = ["app_mentions:read", "channels:history", "channels:read", "chat:write", "commands", "groups:history", "groups:read", "groups:write", "im:history", "im:read"]
_POSTS_AND_ARTICLES = [renderer._ARTICLE, renderer._POST]

oauth_settings = OAuthSettings(
    client_id=config.get_slack_client_id(),
    client_secret=config.get_slack_client_secret(),
    scopes=_SLACK_SCOPES,
    installation_store=slack_stores.MongoInstallationStore(conn_str=config.get_db_connection_string(), app_name="espresso"),
    state_store=slack_stores.MongoOauthStateStore(conn_str=config.get_db_connection_string(), app_name="espresso", expiration_seconds=600)
)

# set up the initial app
app = App(
    # token=config.get_slack_bot_token(),
    signing_secret=config.get_slack_signing_secret(),
    oauth_settings=oauth_settings
)

# @app.message()
# def receive_message(message, say, client): 
#     receiver.new_message(
#         message_or_event = message,         
#         say = say, 
#         client = client
#     )

# @app.event("app_mention")
# def receive_mention(event, say, client):
#     receiver.new_message(
#         message_or_event = event,        
#         say=say, 
#         client=client
#     )


_MAX_DISPLAY_BATCH_SIZE = 3

is_one_block = lambda data: data and isinstance(data, list) and isinstance(data[0], dict)
is_list_of_blocks = lambda data: data and isinstance(data, list) and isinstance(data[0], list)
is_text = lambda data: isinstance(data, str)

class ChannelManager:
    queues: dict[str, queue.Queue]
    clients: dict[str, any]

    def __init__(self):
        self.queues = {}

    def _get_channel(self, channel_id: str = None, channel_type: str = None, user_id: str = None, create_new = True) -> str:
        if channel_type == "directmessage":
            channel_id = user_id
        elif not channel_id:
            channel_id = user_id
        if create_new or (not self.queues.get(channel_id)):
            self.queues[channel_id] = queue.Queue()
        return channel_id
            
    # blocks can be: 
    #   - list[list[dict]]: represents an array of blocks where each set of blocks represents a news item
    #   - list[dict]: represents one news or display item
    #   - str: represents one text message
    def _queue_blocks(self, blocks, client, channel_id: str = None, channel_type: str = None, user_id: str = None):
        channel_id = self._get_channel(channel_id=channel_id, channel_type=channel_type, user_id=user_id, create_new=True)        
        if is_text(blocks) or is_one_block(blocks):
            # list[dict] or str --> represents 1 item
            self.queues[channel_id].put(blocks) 
        elif is_list_of_blocks(blocks):
            # list[list[dict]] --> represents a list of blocks, so merge them
            blocks = [list(chain(*blocks[i:i+_MAX_DISPLAY_BATCH_SIZE])) for i in range(0, len(blocks), _MAX_DISPLAY_BATCH_SIZE)]
            [self.queues[channel_id].put(item) for item in blocks]
        # don't do anything if the blocks are empty                    
   
    # returns a merge batch of blocks to display the batch to display        
    def _dequeue_blocks(self, channel_id: str) -> list[dict]|str:
        return self.queues[channel_id].get_nowait() if not self.queues[channel_id].empty() else None
    
    def next_page(self, client = None, channel_id: str = None, channel_type: str = None, user_id: str = None):
        channel_id = self._get_channel(channel_id=channel_id, channel_type=channel_type, user_id=user_id, create_new=False)
        page = self._dequeue_blocks(channel_id)

        if is_one_block(page):  
            # this is a set of blocks 
            client.chat_postMessage(channel=channel_id, text=f"Displaying items.", blocks=page)        
            remaining = self.queues[channel_id].qsize()        
            if remaining:
                client.chat_postMessage(channel=channel_id, text=f"There are {remaining} more item(s). Run */more* for more.")  
        elif is_text(page):
            client.chat_postMessage(channel=channel_id, text=page)                        
        else:
            client.chat_postMessage(channel=channel_id, text=renderer.NO_MORE_CONTENT)

    def publish(self, blocks, client = None,  channel_id: str = None, channel_type: str = None, user_id: str = None):
        self._queue_blocks(blocks = blocks, client = client, channel_id=channel_id, channel_type=channel_type, user_id=user_id)
        self.next_page(client = client, channel_id=channel_id, channel_type=channel_type, user_id=user_id)

channel_mgr = ChannelManager()      

@app.event("app_home_opened")
def update_home_tab(event, client):
    if event['tab'] == "home":        
        _refresh_home_tab(event['user'], client)

@app.command("/trending")
def receive_trending(ack, command, client, say):
    ack()    
    channel_mgr.publish(
        client = client, 
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'],
        blocks = renderer.get_trending_items(user_id=command['user_id'], params=command['text'].split(' ')))

@app.command("/more")
def receive_more(ack, command, client, say):
    ack()
    channel_mgr.next_page(
        client = client,
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'])
    
@app.command("/lookfor")
def receive_lookfor(ack, command, client, say):
    ack()        
    channel_mgr.publish(
        client = client, 
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'],
        blocks = renderer.get_beans_by_search(user_id=command['user_id'], search_text=command['text']))

@app.action(re.compile("^nugget//*"))
def receive_nugget_search(ack, action, client):
    ack()
    vals = action['value'].split("//")
    keyphrase, description, user_id, window = vals[0], vals[1], vals[2], vals[3]

    channel_mgr.publish(
        client = client, 
        user_id=user_id,
        blocks=renderer.get_beans_by_nugget(
            user_id=user_id, 
            keyphrase=keyphrase, 
            description=description, 
            show_by_preference=("show_by_preference" in action['action_id'].split("//")),
            window=window))

@app.action(re.compile("^category//*"))
def receive_category_search(ack, action, client):
    ack()
    vals = action['value'].split("//")
    channel_mgr.publish(
        client = client, 
        user_id=vals[1],
        blocks = renderer.get_beans_by_category(user_id=vals[1], category=vals[0]))

# @app.action(re.compile("^connect:*"))
# def receive_connect(ack):
#     ack()

@app.action(re.compile("^update_interests:*"))
def trigger_update_interest(ack, action, body, client):
    ack()
    client.views_open(
        trigger_id=body['trigger_id'],
        view = renderer.UPDATE_INTEREST_VIEW
    )

@app.view("new_interest_input")
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
