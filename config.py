import os
from icecream import ic

SLACK_SCOPES = ["app_mentions:read", "channels:history", "channels:read", "chat:write", "commands", "groups:history", "groups:read", "groups:write", "im:history", "im:read", "reactions:read", "reactions:write", "users:read"]

def get_slack_app_name() -> str:
    return os.getenv("ESPRESSO_APP_NAME")

def get_slack_bot_token() -> str:
    return os.getenv("ESPRESSO_SLACK_BOT_TOKEN")

def get_slack_app_token() -> str:
    return os.getenv("ESPRESSO_SLACK_APP_TOKEN")

def get_slack_signing_secret() -> str:
    return os.getenv("ESPRESSO_SLACK_SIGNING_SECRET")

def get_slack_client_id():
    return os.getenv("ESPRESSO_SLACK_CLIENT_ID")

def get_slack_client_secret():
    return os.getenv("ESPRESSO_SLACK_CLIENT_SECRET")

# def get_slack_oauth_redirect_uri():
#     return os.getenv("ESPRESSO_SLACK_REDIRECT_URI")

def get_beansack_url() -> str:
    return os.getenv("BEANSACK_URL")

def get_redditor_url() -> str:
    return os.getenv("REDDITOR_URL")

def get_internal_auth_token() -> str:
    return os.getenv("INTERNAL_AUTH_TOKEN")

def get_db_connection_string():
    return os.getenv("DB_CONNECTION_STRING")

# REDDIT_OAUTH_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
# REDDIT_OAUTH_AUTHORIZE_URL = "https://www.reddit.com/api/v1/authorize"
# REDDIT_OAUTH_REDIRECT_URL="https://workable-feline-deeply.ngrok-free.app/reddit/oauth_redirect"

# def get_reddit_app_secret():
#     return os.getenv("ESPRESSO_REDDIT_APP_SECRET")

# def get_reddit_app_id():
#     return os.getenv("ESPRESSO_REDDIT_APP_ID")