import os

APP_NAME="Espresso by Cafecit.io"

# sources
SLACK = "SLACK"
REDDIT = "REDDIT"

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

def get_nicegui_storage_secret():
    return os.getenv("INTERNAL_AUTH_TOKEN")
def get_db_connection_str():
    return os.getenv("DB_CONNECTION_STRING")
def get_llm_api_key():
    return os.getenv("GROQ_API_TOKEN")
def get_embedder_model_path():
    return os.getenv("EMBEDDER_PATH")
