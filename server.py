from slack_ui.router import slack_router
from flask import Flask, request
from slack_bolt.adapter.flask import SlackRequestHandler

# running in HTTP mode
server = Flask(__name__)
handler = SlackRequestHandler(slack_router)

@server.route("/slack/events", methods=["POST"])
@server.route("/slack/commands", methods=["POST"])
@server.route("/slack/actions", methods=["POST"])
@server.route("/slack/oauth_redirect")
@server.route("/slack/install")
def slack_events():
    return handler.handle(request)

if __name__ == "__main__":
    server.run(port=8080)
