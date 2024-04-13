from datetime import datetime
import requests
import requests.auth
import config 
import userops
from icecream import ic
import urllib.parse
import redditor

_FIRE_MIN = 10
_SLACK = "SLACK"
_ARTICLE="article"
_POST="post"
_CHANNEL="channel"

def get_user_home_blocks(user_id):
    return _create_home_blocks(
        user_id = user_id,
        interests = get_user_preferences(user_id), 
        trending_day = get_topics_blocks(user_id=user_id, window = 1), 
        trending_week = get_topics_blocks(user_id=user_id, window = 7, limit=10))

def get_trending_items_blocks(user_id: str, params: list[str]):
    params = [p.strip().lower() for p in params if p.strip()]
    if (len(params) == 0) or ("topics" in params):
        # show everything that is trending regardless of interest/preference
        return get_topics_blocks(user_id=user_id)

    # or else pull in the preference and show the type of items the user wants
    prefs = userops.get_preferences(source=_SLACK, username=user_id)

    if "news" in params:
        return get_beans_blocks(user_id=user_id, query_texts=prefs, kinds=[_ARTICLE], window=1)
    elif "posts" in params:
        return get_beans_blocks(user_id=user_id, query_texts=prefs, kinds=[_POST], window=1)
    elif "channels" in params:
        return get_beans_blocks(user_id=user_id, query_texts=prefs, kinds=[_CHANNEL], window=1)

        
def get_beans_blocks(user_id, keywords = None, query_texts = None, kinds: list[str] = None, window = 1, limit: int = 5):    
    res = get_beans(keywords = keywords, query_texts=query_texts, kinds = kinds, window = window)
    return _create_bean_blocks(user_id, res)

def get_topics_blocks(user_id, window: int = 1, limit: int = 5):    
    res = get_topics(window)[:limit]
    return _create_topic_blocks(user_id, res, window=window)

def _create_topic_blocks(user_id, topics, window=1):
    body_text = lambda data: data.get('keyword') if data.get('Count') < _FIRE_MIN else f"{data.get('keyword')} :fire:"
    body = lambda data: {
		"type": "button",
		"text": {
			"type": "plain_text",
			"text": body_text(data),
			"emoji": True
		},
		"value": f"{data.get('keyword')}//{user_id}//{window}",
		"action_id": f"get_beans:{data.get('keyword')})"
	}
    return [
        {
            "type": "actions",
		    "elements": [body(keyword) for keyword in topics]
        }
    ]

def _create_bean_blocks(userid, beans):
    date_element = lambda data: {
        "type": "plain_text",
        "text": f":date: {datetime.fromtimestamp(data.get('created') if data.get('created') else data.get('updated')).strftime('%b %d, %Y')}"
    }
    source_element = lambda data: {
        "type": "mrkdwn",
		"text": f":link: <{data.get('url')}|{data.get('source')}>"
    }
    author_element = lambda data: {
        "type": "plain_text",
		"text": f":writing_hand: {data.get('author')}" 
    }

    # fix reddit specific show
    banner = lambda data: {
        "type": "context",
        "elements": [source_element(data), date_element(data), author_element(data)] if data.get("author") else [source_element(data), date_element(data)]
    }


    body = lambda data: {        
		"type": "section",
		"text": {
			"type": "mrkdwn",
			"text": f"*{data.get('title', '')}\n*{data.get('summary')}" if data.get('summary') else f"*{data.get('title', '')}*"
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
    if beans:
        return [[banner(item), body(item), action(item)] for item in beans]

def _create_home_blocks(user_id, interests, trending_day, trending_week):
    # THINGS TO SHOW
    # VIEW
    # [DONE] 1. Top 5 trending keywords/topics - 1 day, 1 week
    # 2. Social Media Stats - last 1 week
    # EDIT
    # [DONE] 1. Interests
    # [DONE] 2. Login to Reddit
    # 3. Login to LinkedIn 
    interests_header = [
        {
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": "Your Interests"
			}
		}
    ] 
    trending_news_header = [
        {
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": "Trending In News"
			}
		}
    ]
    one_day_header = [
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*Since Yesterday*"
			}
		}
    ] 
    one_week_header = [
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*For A Week*"
			}
		}
    ]
    divider = [
        {
			"type": "divider"
		}
    ]

    reddit_status = redditor.is_user_authenticated(user_id)
    if reddit_status != True:
        reddit_element = {
			"type": "actions",
			"elements": [
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "Reddit"
					},
					"value": "reddit",
					"url": reddit_status,
					"action_id": "connect:reddit"
				}
			]
		}
    else:
        reddit_element = {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*Reddit* : Connected :large_green_circle: (Your soul now belongs to us)"
			}
		}

    connect = [
        {
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": "Connect Your Accounts"
			}
		},
        reddit_element,
        {
			"type": "actions",
			"elements": [
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "LinkedIn",
						"emoji": True
					},
					"value": "linkedin",
					"url": "http://www.linkedin.com",
					"action_id": "connect:linkedin"
				}
			]
		}
    ]
    return interests_header + _create_interests_blocks(user_id, interests) + divider + trending_news_header + one_day_header + trending_day + one_week_header + trending_week + divider + connect

def _create_interests_blocks(user_id, interests):
    interest_button = lambda data: {
		"type": "button",
		"text": {
			"type": "plain_text",
			"text": data
		},
		"value": f"{data}//{user_id}",
		"action_id": f"search_beans:{data})"
	}
    update_button = {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": "Update",
            "emoji": False
        },
        "style": "primary",
        "value": user_id,
        "action_id": f"update_interests:{user_id}"
    }
    if interests:
        return [
            {
                "type": "actions",
                "elements": [interest_button(item) for item in interests] + [update_button]
            }
        ]
    else:
        return [
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": "Nothing specified."
                },
                "accessory": update_button
            }
        ]

# def _create_reddit_oauth_request_url(user_id) -> str:
#     params = {
#         "client_id": config.get_reddit_app_id(),
# 		"response_type": "code",
# 		"state": user_id,
# 		"redirect_uri": config.REDDIT_OAUTH_REDIRECT_URL,
# 		"duration": "permanent",
# 		"scope": "identity"
#     }
#     return f"{config.REDDIT_OAUTH_AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"

def get_user_preferences(user_id):
    return userops.get_preferences(_SLACK, user_id)

def update_user_preferences(user_id: str, interests: list[str]):
    userops.update_preferences(_SLACK, user_id, interests)    

_TRENDING_BEANS = "/beans/trending"
_SEARCH_BEANS = "/beans/search"
_TRENDING_TOPICS = "/topics/trending"

def get_beans(keywords: list[str] = None, query_texts: str|list[str] = None, search_context: str = None, kinds:list[str] = None, window: int = 1):
    params = {
        "window": window,
    }
    if kinds:
        params.update({"kind": kinds})
    if keywords:
        params.update({"keyword": keywords})

    if query_texts:
        body = {"query_texts": query_texts if isinstance(query_texts, list) else [query_texts]}
        resp = requests.get(config.get_beansack_url()+_SEARCH_BEANS, json=body, params=params)
    elif search_context:
        body = {"search_context": search_context}
        resp = requests.get(config.get_beansack_url()+_SEARCH_BEANS, json=body, params=params)
    else:        
        resp = requests.get(config.get_beansack_url()+_TRENDING_BEANS, params=params)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []


def get_topics(window: int = 1):
    params = {"window": window}
    resp = requests.get(config.get_beansack_url()+_TRENDING_TOPICS, params=params)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []