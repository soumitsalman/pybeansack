import re
import renderer
import config
from icecream import ic
from slack_bolt import App
from slack_bolt.oauth.oauth_settings import OAuthSettings
import slack_stores
import queue

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
    
    def dequeue_blocks(self, channel_id: str):
        i = _MAX_DISPLAY_BATCH_SIZE
        batch = []
        while i and (not self.queues[channel_id].empty()):
            batch.extend(self.queues[channel_id].get())
            i -= 1
        return batch, _MAX_DISPLAY_BATCH_SIZE - i
    
    def queue_blocks(self, blocks, client, channel_id: str = None, channel_type: str = None, user_id: str = None):
        channel_id = self._get_channel(channel_id=channel_id, channel_type=channel_type, user_id=user_id, create_new=True)      
        # if this is an array of item
        if blocks:
            if len(blocks) > 0 and isinstance(blocks[0], list):            
                for item in blocks:
                    self.queues[channel_id].put(item)
            # if this is only 1 item
            else:
                self.queues[channel_id].put(blocks)  
        # don't do anything if the blocks are empty
  
    
    def display_blocks(self, client, channel_id: str = None, channel_type: str = None, user_id: str = None):
        channel_id = self._get_channel(channel_id=channel_id, channel_type=channel_type, user_id=user_id, create_new=False)
        batch, length = self.dequeue_blocks(channel_id)
        if length:
            remaining = self.queues[channel_id].qsize()
            client.chat_postMessage(channel=channel_id, text=f"Displaying {length} item(s).", blocks=batch)
            if remaining:
                client.chat_postMessage(channel=channel_id, text=f"There are {remaining} more item(s). Run */more* for more.")
        else:
            client.chat_postMessage(channel=channel_id, text="No content :shrug:")

    def queue_and_display_blocks(self, blocks, client, channel_id: str = None, channel_type: str = None, user_id: str = None):
        self.queue_blocks(blocks = blocks, client = client, channel_id=channel_id, channel_type=channel_type, user_id=user_id)
        self.display_blocks(client = client, channel_id=channel_id, channel_type=channel_type, user_id=user_id)

channel_mgr = ChannelManager()      

@app.event("app_home_opened")
def update_home_tab(event, client):
    if event['tab'] == "home":        
        _refresh_home_tab(event['user'], client)

@app.command("/trending")
def receive_trending(ack, command, client):
    ack()    
    channel_mgr.queue_and_display_blocks(
        client = client, 
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'],
        blocks = renderer.get_trending_items_blocks(user_id=command['user_id'], params=command['text'].split(' ')))

@app.command("/more")
def receive_more(ack, command, client):
    ack()
    channel_mgr.display_blocks(
        client = client,
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'])
    
@app.command("/lookfor")
def receive_search(ack, command, client):
    ack()    
    channel_mgr.queue_and_display_blocks(
        client = client, 
        channel_id = command['channel_id'],
        channel_type=command['channel_name'], 
        user_id=command['user_id'],
        blocks = renderer.get_beans_blocks(user_id=command['user_id'], search_text=command['text'], kinds = _POSTS_AND_ARTICLES, window=1, limit=10))

@app.action(re.compile("^get_beans//*"))
def receive_getbeans(ack, action, client):
    ack()
    vals = action['value'].split("//")
    keyphrase, description, user_id, window = vals[0], vals[1], vals[2], vals[3]
    
    # if the action is coming from the home page then display the description before pulling up the news
    if "from_home" in action['action_id'].split("//"):
        channel_mgr.queue_and_display_blocks(
            client = client,
            user_id = user_id,
            blocks=renderer.make_nugget_block(keyphrase, description)
        )

    channel_mgr.queue_and_display_blocks(
        client = client, 
        user_id=user_id,
        blocks=renderer.get_beans_blocks(user_id=user_id, nugget=keyphrase, kinds=_POSTS_AND_ARTICLES, window=window))

@app.action(re.compile("^query_beans:*"))
def receive_searchbeans(ack, action, client):
    ack()
    vals = action['value'].split("//")
    channel_mgr.queue_and_display_blocks(
        client = client, 
        user_id=vals[1],
        blocks = renderer.get_beans_blocks(user_id=vals[1], categories=vals[0], kinds=_POSTS_AND_ARTICLES, window=1))

@app.action(re.compile("^connect:*"))
def receive_connect(ack):
    ack()

@app.action(re.compile("^update_interests:*"))
def trigger_update_interest(ack, action, body, client):
    ack()
    client.views_open(
        trigger_id=body['trigger_id'],
        view = renderer.make_update_interest_view()
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
            "blocks": renderer.get_user_home_blocks(user_id)
        }
    )
