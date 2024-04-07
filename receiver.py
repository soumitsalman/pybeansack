import re
import provider
import config
from icecream import ic
from itertools import chain
from slack_bolt import App
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store import FileInstallationStore
from slack_sdk.oauth.state_store import FileOAuthStateStore
import queue

oauth_settings = OAuthSettings(
    client_id=config.get_slack_client_id(),
    client_secret=config.get_slack_client_secret(),
    scopes=config.SLACK_SCOPES,
    installation_store=FileInstallationStore(base_dir="./data/installations"),
    state_store=FileOAuthStateStore(expiration_seconds=3600, base_dir="./data/states")
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


_MAX_BATCH_SIZE = 3

class ChannelManager:
    queues: dict[str, queue.Queue]
    clients: dict[str, any]

    def __init__(self):
        self.queues = {}
        self.clients = {}

    def queue_msg(self, client, channel_id: str, blocks, override = True):
        if override or (not self.queues[channel_id]):
            self.queues[channel_id] = queue.Queue()
            self.clients[channel_id] = client
        # if this is an array of item
        if len(blocks) > 0 and isinstance(blocks[0], list):            
            for item in blocks:
                self.queues[channel_id].put(item)
        # if this is only 1 item
        elif blocks:
            self.queues[channel_id].put(blocks)    
    
    def display_msg(self, channel_id: str):
        if not self.queues[channel_id].empty():
            self.clients[channel_id].chat_postMessage(channel=channel_id, text="1 item", blocks=self.queues[channel_id].get())
        else:
            self.clients[channel_id].chat_postMessage(channel=channel_id, text="Nothing found")

    def queue_and_display_msg(self, client, channel_id: str, blocks, override = True):
        self.queue_msg(client = client, channel_id=channel_id, blocks=blocks, override=override)
        self.display_msg(channel_id=channel_id)

channel_mgr = ChannelManager()      

@app.event("app_home_opened")
def update_home_tab(event, client):
    if event['tab'] == "home":        
        _refresh_home_tab(event['user'], client)

@app.command("/trending")
def receive_whatsnew(ack, command, client):
    ack()    
    channel_mgr.queue_and_display_msg(
        client = client, 
        channel_id = command['user_id'],
        blocks = provider.get_trending_items_blocks(user_id=command['user_id'], params_text=command['text']))

@app.command("/more")
def receive_whatsnew(ack, command):
    ack()
    channel_mgr.display_msg(channel_id = command['user_id'])

@app.action(re.compile("^get_beans:*"))
def receive_getbeans(ack, action, client):
    ack()
    vals = action['value'].split("//")
    channel_mgr.queue_and_display_msg(
        client = client, 
        channel_id=vals[1],
        blocks=provider.get_beans_blocks(user_id=vals[1], keywords=[vals[0]], window=vals[2]))

@app.action(re.compile("^search_beans:*"))
def receive_searchbeans(ack, action, client):
    ack()
    vals = action['value'].split("//")
    channel_mgr.queue_and_display_msg(
        client = client, 
        channel_id=vals[1],
        blocks = provider.get_beans_blocks(user_id=vals[1], query_texts=vals[0]))

@app.action(re.compile("^connect:*"))
def receive_connect(ack):
    ack()

@app.action(re.compile("^update_interests:*"))
def trigger_update_interest(ack, action, body, client):
    ack()
    client.views_open(
        trigger_id=body['trigger_id'],
        view = {
            "type": "modal",
            "callback_id": "new_interest_input",
            "title": {"type": "plain_text", "text": "Espresso by Cafecit.io"},
            "submit": {"type": "plain_text", "text": "Update"},
            "blocks": [
                {
                    "block_id": "new_interest_input",
                    "type": "input",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "new_interests"
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Your Interests (comma separated)"
                    }
                }
            ]
        }
    )

@app.view("new_interest_input")
def new_interests(ack, body, view, client):
    ack()
    user_id = body["user"]["id"]
    interests = view["state"]["values"]["new_interest_input"]["new_interests"]['value']
    # update database
    provider.update_user_preferences(user_id=user_id, interests=[item.strip().lower() for item in interests.split(',') if item.strip()])
    # update home view
    _refresh_home_tab(user_id, client)

def _refresh_home_tab(user_id, client):
    client.views_publish(
        user_id = user_id,
        view = {
            "type": "home",
            "blocks": provider.get_user_home_blocks(user_id)
        }
    )

# def _display_blocks(blocks, client, channel_id):
#     # check if it is an array of an array
#     if len(blocks) > 0 and isinstance(blocks[0], list):
#         # for disp_block in blocks:
#         client.chat_postMessage(channel=channel_id, text=f"{len(blocks)} Items", blocks=list(chain.from_iterable(blocks)))
#     # or else if there is any element in it
#     elif blocks:
#         client.chat_postMessage(channel=channel_id, text="1 Item", blocks=blocks)
#     else:
#         client.chat_postMessage(channel=channel_id, text="No items found")
