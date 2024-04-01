from datetime import datetime
import requests
import requests.auth
import config 
from icecream import ic
import urllib.parse

_FIRE_MIN = 10

def get_user_home_data(user_id):
    return _create_home_blocks(
        user_id = user_id,
        interests = get_user_interests(user_id), 
        trending_day = get_topics(window = 1), 
        trending_week = get_topics(window = 7))

def get_user_interests(user_id):
    #  TODO: pull from database
    return ["Data Breach", "llm", "gen ai"]

def update_user_interests(user_id: str, interests: list[str]):
    ic(user_id, interests)

def get_trending_items(msg_or_event):
    params = [p.lower() for p in msg_or_event['text'].split(" ")]
    if "beans" in params:
        return get_beans()
    else:
        return get_topics()

def get_beans(topics = None, query_texts = None, limit: int = 5):    
    res = trending_beans(topics = topics, query_texts=query_texts)[:limit]
    return _create_bean_blocks(res)

def get_topics(window: int = 1, limit: int = 5):    
    res = trending_topics(window)[:limit]
    return _create_topic_blocks(res)

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
        ic(user_id, resp_body['access_token'])        
        return "auth succeeded", resp.status_code
    else:
        return resp_body, resp.status_code


def _create_topic_blocks(topics):
    body_text = lambda data: data.get('keyword') if data.get('Count') < _FIRE_MIN else f"{data.get('keyword')} :fire:"
    body = lambda data: {
		"type": "button",
		"text": {
			"type": "plain_text",
			"text": body_text(data),
			"emoji": True
		},
		"value": data.get('keyword'),
		"action_id": f"get_beans({data.get('keyword')})"
	}
    return [
        {
            "type": "actions",
		    "elements": [body(keyword) for keyword in topics]
        }
    ]

def _create_bean_blocks(beans):
    date_element = lambda data: {
        "type": "plain_text",
        "text": f":date: {datetime.fromtimestamp(data.get('created') if data.get('created') else data.get('updated')).strftime('%b %d, %Y')}"
    }
    source_element = lambda data: {
        "type": "mrkdwn",
		"text": f":link: <{data.get('url')}|{data.get('source')}>"
    }

    banner = lambda data: {
        "type": "context",
        "elements": [            
            # {
            #     "type": "plain_text",
            #     "text": f":thumbsup: {data.get('likes', 0)}"
            # },
            # {
            #     "type": "plain_text",
            #     "text": f":left_speech_bubble: {data.get('comments', 0)}"
            # },   
            source_element(data),                            
            # tags_element(data),
            date_element(data)
        ]
    }
    body = lambda data: {        
		"type": "section",
		"text": {
			"type": "mrkdwn",
			"text": f"*:rolled_up_newspaper: {data.get('title', '')}*\n{data.get('summary')}"
		}
    }
    
    action = lambda data: {    
		"type": "actions",
		"elements": [
			{
                "action_id": f"positive",
                "type": "button",
				"text": {
					"type": "plain_text",
					"text": ":ok_hand:",
                    "emoji": True
				},
				"value": data.get('url')
			},
			{
                "action_id": f"negative",
                "type": "button",
				"text": {
					"type": "plain_text",
					"text": ":shit:",
                    "emoji": True
				},
				"value": data.get('url')
			}
		]
	}
    
    return [[banner(item), body(item), action(item)] for item in beans]

def _create_home_blocks(user_id, interests, trending_day, trending_week):
    # THINGS TO SHOW
    # VIEW
    # [DONE] 1. Top 5 trending keywords/topics - 1 day, 1 week
    # 2. Social Media Stats - last 1 week
    # EDIT
    # [HALF DONE] 1. Interests
    # [DONE] 2. Login to Reddit
    # 3. Login to LinkedIn 
    interests_header = [
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*Your Interests*"
			}
		}
    ] 
    one_day_header = [
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*Trending Since Yesterday*"
			}
		}
    ] 
    one_week_header = [
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*Trending For A Week*"
			}
		}
    ]
    divider = [
        {
			"type": "divider"
		}
    ]
    # update_interests = [        
	# 	{
    #         "dispatch_action": True,
	# 		"type": "input",
    #         "block_id": user_id,
	# 		"element": {
	# 			"type": "plain_text_input",
	# 			"action_id": "modify_interests"
	# 		},
	# 		"label": {
	# 			"type": "plain_text",
	# 			"text": "Your Interests",
	# 		}
	# 	}
    # ]

    connect = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Connect Your Account*"
            }
        },
        {
			"type": "actions",
			"elements": [
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "Reddit",
						"emoji": True
					},
					"value": "reddit",
					"url": _create_reddit_oauth_request_url(user_id),
					"action_id": "connect_reddit"
				},
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "LinkedIn",
						"emoji": True
					},
					"value": "linkedin",
					"url": "http://www.linkedin.com",
					"action_id": "connect_linkedin"
				}
			]
		}
    ]
    return interests_header + _create_interests_blocks(interests) + divider + one_day_header + trending_day + one_week_header + trending_week + divider + connect

def _create_interests_blocks(interests):
    body = lambda data: {
		"type": "button",
		"text": {
			"type": "plain_text",
			"text": data
		},
		"value": data,
		"action_id": f"search_beans({data})"
	}
    return [
        {
            "type": "actions",
		    "elements": [body(item) for item in interests]
        }
    ]

def _create_reddit_oauth_request_url(user_id) -> str:
    params = {
        "client_id": config.get_reddit_app_id(),
		"response_type": "code",
		"state": user_id,
		"redirect_uri": config.REDDIT_OAUTH_REDIRECT_URL,
		"duration": "permanent",
		"scope": "identity"
    }
    return f"{config.REDDIT_OAUTH_AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"

_TRENDING_BEANS = "/trending/beans"
_SEARCH_BEANS = "/trending/beans/search"
_TRENDING_TOPICS = "/trending/topics"

def trending_beans(topics = None, query_texts = None, window: int = 1):
    params = {"window": window}
    if query_texts:
        body = {"query_texts": query_texts}
        resp = requests.get(config.get_beansack_url()+_SEARCH_BEANS, data=body, params=params)
    elif topics:
        params.update({"topic":topic for topic in topics})
        resp = requests.get(config.get_beansack_url()+_TRENDING_BEANS, params=params)
    else:        
        resp = requests.get(config.get_beansack_url()+_TRENDING_BEANS, params=params)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []

def trending_topics(window: int = 1):
    params = {"window": window}
    resp = requests.get(config.get_beansack_url()+_TRENDING_TOPICS, params=params)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []