import os
from slack_ui.router import slack_router
from flask import Flask, request
from slack_bolt.adapter.flask import SlackRequestHandler
import logging

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
    mode = os.getenv("INSTANCE_MODE")
    if mode == "WEB":   
        logging.info("Running in WEB UI Mode")
        from web_ui import router 
        router.load_webui()
    elif mode == "SLACK":
        logging.info("Running in SLACK UI Mode")
        server.run(port=8080)
    else:
        logging.error("WTF IS THIS? Exiting ...")
