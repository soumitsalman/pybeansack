import requests
import requests.auth
import config
import urllib
from icecream import ic

def get_reddit_user_token(user_id, code):
    client_auth = requests.auth.HTTPBasicAuth(config.get_reddit_app_id(), config.get_reddit_app_secret())
    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": config.REDDIT_OAUTH_REDIRECT_URL
    }
    headers = { 
        "User-Agent": config.APP_NAME,
        "Content-Type":  "application/x-www-form-urlencoded"
    }
    resp = requests.post(config.REDDIT_OAUTH_TOKEN_URL, auth=client_auth, data=urllib.parse.urlencode(body), headers=headers)
    resp_body = resp.json()
    if resp.status_code == requests.codes["ok"]:                
        ic(user_id, resp_body)        
        return "auth succeeded", resp.status_code
    else:
        return resp_body, resp.status_code