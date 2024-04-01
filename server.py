import receiver
import config
from icecream import ic
from slack_bolt import App
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store import FileInstallationStore
from slack_sdk.oauth.state_store import FileOAuthStateStore

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
                "blocks": receiver.display_user_data(event)
            }
        )

@app.command("/whatsnew")
def receive_whatsnew(command, respond, ack):
    ack()
    ic("/whatsnew", command['user_name'], command['text'])
    
    # items = get_new_items(command['user_name'], command['text'] )
    for disp_block in receiver.get_beans(command):
        respond(blocks = disp_block)

# running in HTTP mode
server = Flask(__name__)
handler = SlackRequestHandler(app)

@server.route("/slack/events", methods=["POST"])
@server.route("/slack/commands", methods=["POST"])
@server.route("/slack/actions", methods=["POST"])
@server.route("/slack/oauth_redirect", methods=["GET"])
@server.route("/slack/install", methods=["GET"])
def slack_events():
    return handler.handle(request)

if __name__ == "__main__":
    server.run(port=8080)
