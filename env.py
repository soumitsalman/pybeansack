from dotenv import load_dotenv
import os

load_dotenv()

def db_connection_str():
    return os.getenv("DB_CONNECTION_STRING")
def sb_connection_str():
    return os.getenv("SB_CONNECTION_STRING")

def llm_base_url():
    return os.getenv("LLM_BASE_URL")
def llm_api_key():
    return os.getenv("LLM_API_KEY")
def embedder_model():
    return os.getenv("EMBEDDER_MODEL")
def embedder_n_ctx():
    return int(os.getenv("EMBEDDER_N_CTX"))

def reddit_client_id():
    return os.getenv("REDDITOR_APP_ID")
def reddit_client_secret():
    return os.getenv("REDDITOR_APP_SECRET")

def slack_bot_token() -> str:
    return os.getenv("SLACKER_BOT_TOKEN")
def slack_app_token() -> str:
    return os.getenv("SLACKER_APP_TOKEN")
def slack_signing_secret() -> str:
    return os.getenv("SLACKER_SIGNING_SECRET")
def slack_client_id():
    return os.getenv("SLACKER_CLIENT_ID")
def slack_client_secret():
    return os.getenv("SLACKER_CLIENT_SECRET")

def base_url():
    return os.getenv("BASE_URL")
def internal_auth_token():
    return os.getenv("INTERNAL_AUTH_TOKEN")
