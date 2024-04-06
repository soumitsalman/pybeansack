import re
import time
import provider
import config
from icecream import ic
from itertools import chain
from slack_bolt import App
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store import FileInstallationStore
from slack_sdk.oauth.state_store import FileOAuthStateStore

from requests import status_codes
from flask import Flask, request
from slack_bolt.adapter.flask import SlackRequestHandler
# from slack_bolt.adapter.socket_mode import SocketModeHandler   

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

@app.event("app_home_opened")
def update_home_tab(event, client):
    if event['tab'] == "home":        
        _refresh_home_tab(event['user'], client)

def _refresh_home_tab(user_id, client):
    client.views_publish(
        user_id = user_id,
        view = {
            "type": "home",
            "blocks": provider.get_user_home_data(user_id)
        }
    )

@app.command("/whatsnew")
def receive_whatsnew(ack, command, client):
    ack()
    _display_blocks(provider.get_trending_items(user_id=command['user_id'], params_text=command['text']), client, channel_id = command['channel_id'])

@app.action(re.compile("^get_beans:*"))
def receive_getbeans(ack, action, client):
    ack()
    vals = action['value'].split("//")
    _display_blocks(provider.get_beans(user_id=vals[1], topics=[vals[0]], window=vals[2]), client, channel_id=vals[1])

@app.action(re.compile("^search_beans:*"))
def receive_searchbeans(ack, action, client):
    ack()
    vals = action['value'].split("//")
    _display_blocks(provider.get_beans(user_id=vals[1], query_texts=[vals[0]]), client, channel_id=vals[1])

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
    # provider.update_user_interests(user_id = action['block_id'], interests = [interest.strip() for interest in action['value'].split(',')])

@app.view("new_interest_input")
def new_interests(ack, body, view, client):
    ack()
    user_id = body["user"]["id"]
    interests = view["state"]["values"]["new_interest_input"]["new_interests"]['value']
    # update database
    provider.update_user_interests(user_id=user_id, interests=[item.strip().lower() for item in interests.split(',') if item.strip()])
    # update home view
    _refresh_home_tab(user_id, client)

def _display_blocks(blocks, client, channel_id):
    # check if it is an array of an array
    if len(blocks) > 0 and isinstance(blocks[0], list):
        # for disp_block in blocks:
        client.chat_postMessage(channel=channel_id, text=f"{len(blocks)} Items", blocks=list(chain.from_iterable(blocks)))
    # or else if there is any element in it
    elif blocks:
        client.chat_postMessage(channel=channel_id, text="1 Item", blocks=blocks)
    else:
        client.chat_postMessage(channel=channel_id, text="No items found")

# running in HTTP mode
server = Flask(__name__)
handler = SlackRequestHandler(app)

#  <a href="https://slack.com/oauth/v2/authorize?client_id=15328493825.6458003849858&scope=app_mentions:read,channels:history,channels:join,channels:read,chat:write,conversations.connect:read,conversations.connect:write,groups:history,groups:read,groups:write,im:history,mpim:history,mpim:read,mpim:write,reactions:read,reactions:write,users:read,im:read,commands&user_scope="><img alt="Add to Slack" height="40" width="139" src="https://platform.slack-edge.com/img/add_to_slack.png" srcSet="https://platform.slack-edge.com/img/add_to_slack.png 1x, https://platform.slack-edge.com/img/add_to_slack@2x.png 2x" /></a>
# https://slack.com/oauth/v2/authorize?client_id=15328493825.6458003849858&scope=app_mentions:read,channels:history,channels:join,channels:read,chat:write,conversations.connect:read,conversations.connect:write,groups:history,groups:read,groups:write,im:history,mpim:history,mpim:read,mpim:write,reactions:read,reactions:write,users:read,im:read,commands&user_scope=

@server.route("/slack/events", methods=["POST"])
@server.route("/slack/commands", methods=["POST"])
@server.route("/slack/actions", methods=["POST"])
@server.route("/slack/oauth_redirect", methods=["GET"])
@server.route("/slack/install", methods=["GET"])
def slack_events():
    return handler.handle(request)

@server.route("/reddit/oauth_redirect")
def reddit_oauth():
    error = ic(request.args.get("error"))
    if not error: 
        return provider.get_reddit_user_token(user_id = ic(request.args.get("state")) , code = ic(request.args.get("code")))
    else:
        return error, status_codes.codes["unauthorized"]


if __name__ == "__main__":
    server.run(port=8080)
