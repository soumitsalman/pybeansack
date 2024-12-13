from dotenv import load_dotenv
import os

load_dotenv()

# stuffs from azure/infrastructure
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
SB_CONNECTION_STRING = os.getenv("SB_CONNECTION_STRING")
APPINSIGHTS_CONNECTION_STRING = os.getenv("APPINSIGHTS_CONNECTION_STRING")

# stuffs on LLM API
LLM_BASE_URL = os.getenv("LLM_BASE_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY") 
EMBEDDER_MODEL = os.getenv("EMBEDDER_MODEL")
EMBEDDER_N_CTX = int(os.getenv("EMBEDDER_N_CTX"))

# stuffs on OAuth
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_SERVER_METADATA_URL = 'https://accounts.google.com/.well-known/openid-configuration'
GOOGLE_AUTHORIZE_URL = 'https://accounts.google.com/o/oauth2/auth'
GOOGLE_ACCESS_TOKEN_URL = 'https://oauth2.googleapis.com/token'
GOOGLE_API_BASE_URL = 'https://www.googleapis.com/oauth2/v1/'
GOOGLE_USERINFO_ENDPOINT = 'https://openidconnect.googleapis.com/v1/userinfo'
GOOGLE_OAUTH_SCOPE = {'scope': 'openid email profile'}

SLACK_CLIENT_ID = os.getenv("SLACK_CLIENT_ID")
SLACK_CLIENT_SECRET = os.getenv("SLACK_CLIENT_SECRET")
SLACK_SERVER_METADATA_URL = 'https://slack.com/.well-known/openid-configuration'
SLACK_AUTHORIZE_URL = 'https://slack.com/openid/connect/authorize'
SLACK_ACCESS_TOKEN_URL = 'https://slack.com/api/openid.connect.token'
SLACK_API_BASE_URL = 'https://slack.com/api'
SLACK_OAUTH_SCOPE = {'scope': 'openid profile email'}

LINKEDIN_CLIENT_ID = os.getenv("LINKEDIN_CLIENT_ID")
LINKEDIN_CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")
LINKEDIN_AUTHORIZE_URL = 'https://www.linkedin.com/oauth/v2/authorization'
LINKEDIN_ACCESS_TOKEN_URL = 'https://www.linkedin.com/oauth/v2/accessToken'
LINKEDIN_API_BASE_URL = 'https://api.linkedin.com/v2'
LINKEDIN_OAUTH_SCOPE = {'scope': 'openid profile email'}

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_AUTHORIZE_URL = 'https://www.reddit.com/api/v1/authorize'
REDDIT_ACCESS_TOKEN_URL = 'https://www.reddit.com/api/v1/access_token'
REDDIT_API_BASE_URL = 'https://oauth.reddit.com'
REDDIT_OAUTH_SCOPE = {'scope': 'identity mysubreddits'}

# stuffs on Slack bot
SLACK_BOT_TOKEN = os.getenv("SLACKER_BOT_TOKEN")
SLACK_APP_TOKEN = os.getenv("SLACKER_APP_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACKER_SIGNING_SECRET")

# stuffs on app
BASE_URL = os.getenv("BASE_URL")
APP_NAME = os.getenv("APP_NAME")
APP_STORAGE_SECRET = os.getenv("INTERNAL_AUTH_TOKEN")
IN_MAINTENANCE = bool(os.getenv("IN_MAINTENANCE"))






