import os
import requests
import requests.auth
import shared.config as config
import urllib
from icecream import ic
import praw

def is_user_authenticated(user_id) -> str:
    header = {
        "X-API-Key": config.get_internal_auth_token()
    }
    params = {
        "state": user_id
    }
    # need to fix the actual reddit auth
    resp = requests.get(url = config.get_redditor_url()+"/reddit/auth-status", params=params, headers=header)
    if resp.status_code != requests.codes["ok"]:
        return resp.text
    else:
        return True
    
def collect_user_as_text(username, limit):
    user = ic(create_client().redditor(username))
    text = f"Reddit posts and comments by u/{username}\n\n"
    for post in list(post for post in user.submissions.new(limit=limit*2) if post.is_self)[:limit]:
        text += f"POSTED in r/{post.subreddit.display_name}\n{post.title}\n{post.selftext}\n\n"
    for comment in list(user.comments.new(limit=limit)):
        text += f"COMMENTED in r/{comment.subreddit.display_name}\nOn POST: {comment.submission.title}\n{comment.body}\n\n"
    return text

def create_client():
    return praw.Reddit(
        client_id = os.getenv('REDDITOR_APP_ID'), 
        client_secret = os.getenv('REDDITOR_APP_SECRET'),
        user_agent = "Espresso by Cafecito (by u/randomizer_000)",
        redirect_uri=os.getenv("HOST_URL")+"/reddit/oauth-redirect"
    )



# def get_reddit_user_token(user_id, code):
#     client_auth = requests.auth.HTTPBasicAuth(config.get_reddit_app_id(), config.get_reddit_app_secret())
#     body = {
#         "grant_type": "authorization_code",
#         "code": code,
#         "redirect_uri": config.REDDIT_OAUTH_REDIRECT_URL
#     }
#     headers = { 
#         "User-Agent": config.APP_NAME,
#         "Content-Type":  "application/x-www-form-urlencoded"
#     }
#     resp = requests.post(config.REDDIT_OAUTH_TOKEN_URL, auth=client_auth, data=urllib.parse.urlencode(body), headers=headers)
#     resp_body = resp.json()
#     if resp.status_code == requests.codes["ok"]:                
#         ic(user_id, resp_body)        
#         return "auth succeeded", resp.status_code
#     else:
#         return resp_body, resp.status_code