import redditor
import receiver
from icecream import ic

from requests import status_codes
from flask import Flask, request
from slack_bolt.adapter.flask import SlackRequestHandler

# running in HTTP mode
server = Flask(__name__)
handler = SlackRequestHandler(receiver.app)

@server.route("/slack/events", methods=["POST"])
@server.route("/slack/commands", methods=["POST"])
@server.route("/slack/actions", methods=["POST"])
@server.route("/slack/oauth_redirect")
@server.route("/slack/install")
def slack_events():
    return handler.handle(request)

# @server.route("/reddit/oauth-redirect")
# def reddit_oauth():
#     error = ic(request.args.get("error"))
#     if not error: 
#         return redditor.get_reddit_user_token(user_id = ic(request.args.get("state")) , code = ic(request.args.get("code")))
#     else:
#         return error, status_codes.codes["unauthorized"]


if __name__ == "__main__":
    server.run(port=8080)
