import os

APP_NAME="Espresso by Cafecit.io"
FOUR_HOURS = 14400
ONE_DAY = 86400
ONE_WEEK = 604800
CACHE_SIZE = 100
UNCATEGORIZED = "Yo Momma"
DEFAULT_CATEGORIES = [
    "Artificial Intelligence (AI)", 
    "Automotive & Logistics",
    "Aviation & Aerospace", 
    "Business & Finance", 
    "Career & Professional Development", 
    "Cryptocurrency & Blockchain", 
    "Cybersecurity", 
    "Entrepreneurship & Start-Ups", 
    "Environment & Clean Energy", 
    "Food & Health", 
    "Gadgets & IoT", 
    "Government & Politics", 
    "Management & Leadership", 
    "Robotics & Manufacturing", 
    "Science & Mathematics", 
    "Software Engineering", 
    "Video Games & Virtual Reality",
    UNCATEGORIZED
]
SLACK = "slack"
REDDIT = "reddit"

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

def reddit_client_id():
    return os.getenv("REDDITOR_APP_ID")
def reddit_client_secret():
    return os.getenv("REDDITOR_APP_SECRET")

def db_connection_str():
    return os.getenv("DB_CONNECTION_STRING")
def get_llm_api_key():
    return os.getenv("GROQ_API_TOKEN")
