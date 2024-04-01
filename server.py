import re
import time
import receiver
import config
from icecream import ic
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
        client.views_publish(
            user_id = event['user'],
            view = {
                "type": "home",
                "blocks": receiver.get_user_home_data(event['user'])
            }
        )

@app.command("/whatsnew")
def receive_whatsnew(command, respond, ack):
    ack()
    _display_blocks(receiver.get_trending_items(command), respond)

@app.action(re.compile("^get_beans*"))
def receive_getbeans(action, say, ack):
    ack()
    _display_blocks(receiver.get_beans(topics=[action['value']]), say)

@app.action(re.compile("^connect_*"))
def receive_getbeans(ack):
    ack()

@app.action("modify_interests")
def receive_interests(action, ack):
    ack()
    receiver.update_user_interests(user_id = action['block_id'], interests = [interest.strip() for interest in action['value'].split(',')])

def _display_blocks(blocks, say_or_resp):
    if len(blocks) > 0 and isinstance(blocks[0], list):
        for disp_block in blocks:
            say_or_resp(blocks = disp_block)
    else:
        say_or_resp(blocks = blocks)

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
        return receiver.get_reddit_user_token(user_id = ic(request.args.get("state")) , code = ic(request.args.get("code")))
    else:
        return error, status_codes.codes["unauthorized"]


if __name__ == "__main__":
    server.run(port=8080)
