from datetime import datetime
import requests
import requests.auth
import config 
import userops
from icecream import ic
import pandas as pd
import urllib.parse
import redditor

_FIRE_MIN = 50
_SLACK = "SLACK"
_ARTICLE="article"
_POST="post"
_CHANNEL="channel"

def get_user_home_blocks(user_id):
    prefs = get_user_preferences(user_id)
    return _create_home_blocks(
        user_id = user_id,
        interests = prefs, 
        trending_day = get_nuggets_blocks(user_id, prefs, window=1, limit=5, for_home_page=True), 
        trending_week = get_nuggets_blocks(user_id, prefs, window=7, limit=10, for_home_page=True))

def get_trending_items_blocks(user_id: str, params: list[str]):
    # get the user preference and show the type of items the user wants
    prefs = userops.get_preferences(source=_SLACK, username=user_id)

    params = [p.strip().lower() for p in params if p.strip()]
    if (len(params) == 0) or ("nuggets" in params):
        # show everything that is trending regardless of interest/preference
        return get_nuggets_blocks(user_id=user_id, categories=prefs, window=1, limit=10, for_home_page=False)
    if "news" in params:
        return get_beans_blocks(user_id=user_id, categories=prefs, kinds=[_ARTICLE], window=1, limit=5)
    elif "posts" in params:
        return get_beans_blocks(user_id=user_id, categories=prefs, kinds=[_POST], window=1, limit=5)
    # elif "channels" in params:
    #     return get_beans_blocks(user_id=user_id, categories=prefs, kinds=[_CHANNEL], window=1)
    # elif "topics" in params:
    #     # show everything that is trending regardless of interest/preference
    #     return get_topics_blocks(user_id=user_id)

        
def get_beans_blocks(user_id, nugget = None, categories = None, search_text: str = None, kinds: list[str] = None, window: int = None, limit: int = None):    
    res = get_beans(nugget = nugget, categories=categories, search_text=search_text, kinds = kinds, window = window, limit=limit)
    return _create_bean_blocks(user_id, res)

def get_nuggets_blocks(user_id, categories, window, limit, for_home_page):
    res = get_nuggets(categories, window, limit)
    return _create_nugget_blocks(user_id, res, window, for_home_page)

# def get_topics_blocks(user_id, window: int = 1, limit: int = 5):    
#     res = get_topics(window)[:limit]
#     return _create_topic_blocks(user_id, res, window=window)

def make_nugget_block(keyphrase, description, accessory=None):
    body = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f":rolled_up_newspaper: *{keyphrase}*: {description}",                
        }
    }
    if accessory:
        body["accessory"] = accessory

    return [body]

# def _create_topic_blocks(user_id, topics, window=1):
#     body_text = lambda data: data.get('keyword') if data.get('Count') < _FIRE_MIN else f"{data.get('keyword')} :fire:"
#     body = lambda data: {
# 		"type": "button",
# 		"text": {
# 			"type": "plain_text",
# 			"text": body_text(data),
# 			"emoji": True
# 		},
# 		"value": f"{data.get('keyword')}//{user_id}//{window}",
# 		"action_id": f"get_beans:{data.get('keyword')})"
# 	}
#     return [
#         {
#             "type": "actions",
# 		    "elements": [body(keyword) for keyword in topics]
#         }
#     ]

def _create_nugget_blocks(user_id, nuggets, window, for_home_page):
    #  do a dedup, this is temporary breakfix
    nuggets = pd.DataFrame(nuggets).drop_duplicates(subset=['keyphrase']).to_dict('records')

    value = lambda data: f"{data.get('keyphrase')}//{data.get('description')}//{user_id}//{window}"    
    action_id = lambda data: "get_beans//"+("from_home" if for_home_page else "from_chat")+"//"+data.get('keyphrase')
    if for_home_page:        
        body = lambda data: {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": (":fire: " if data.get("match_count") >= _FIRE_MIN else "") + data.get('keyphrase'),
                "emoji": True
            },
            "value": value(data),
            "action_id": action_id(data)
        }
        return [
            {
                "type": "actions",
                "elements": [body(nugget) for nugget in nuggets]
            }
        ]
        
    else:
        body = lambda data: make_nugget_block(
            data.get('keyphrase'), 
            data.get('description'),
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Pull Up",
                    "emoji": False
                },
                "value": value(data),
                "action_id": action_id(data)
            })
        
        return [body(nugget) for nugget in nuggets]

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
    topic_element = lambda data: {
        "type": "plain_text",
		"text": f":label: {data.get('topic')}" 
    }
    # TODO: fix reddit specific show
    banner = lambda data: {
        "type": "context",
        "elements": [source_element(data), date_element(data)] + ([author_element(data)] if data.get("author") else []) + ([topic_element(data)] if data.get("topic") else [])
    }
    body = lambda data: {        
		"type": "section",
		"text": {
			"type": "mrkdwn",
			"text": f"*{data.get('title').strip()}*\n{data.get('summary').strip() if data.get('summary') else ''}"
		}
    }
    
    # action = lambda data: {    
	# 	"type": "actions",
	# 	"elements": [
	# 		{
    #             "action_id": f"positive",
    #             "type": "button",
	# 			"text": {
	# 				"type": "plain_text",
	# 				"text": ":ok_hand:",
    #                 "emoji": True
	# 			},
	# 			"value": data.get('url')
	# 		},
	# 		{
    #             "action_id": f"negative",
    #             "type": "button",
	# 			"text": {
	# 				"type": "plain_text",
	# 				"text": ":shit:",
    #                 "emoji": True
	# 			},
	# 			"value": data.get('url')
	# 		}
	# 	]
	# }
    if beans:
        return [[banner(item), body(item)] for item in beans]

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

    # TODO: enable later
    # reddit part is working but it is not doing much
    # reddit_status = redditor.is_user_authenticated(user_id)
    # if reddit_status != True:
    #     reddit_element = {
	# 		"type": "actions",
	# 		"elements": [
	# 			{
	# 				"type": "button",
	# 				"text": {
	# 					"type": "plain_text",
	# 					"text": "Reddit"
	# 				},
	# 				"value": "reddit",
	# 				"url": reddit_status,
	# 				"action_id": "connect:reddit"
	# 			}
	# 		]
	# 	}
    # else:
    #     reddit_element = {
	# 		"type": "section",
	# 		"text": {
	# 			"type": "mrkdwn",
	# 			"text": "*Reddit* : Connected :large_green_circle: (Your soul now belongs to us)"
	# 		}
	# 	}

    # connect = [
    #     {
	# 		"type": "header",
	# 		"text": {
	# 			"type": "plain_text",
	# 			"text": "Connect Your Accounts"
	# 		}
	# 	},
    #     reddit_element,
    #     {
	# 		"type": "actions",
	# 		"elements": [
	# 			{
	# 				"type": "button",
	# 				"text": {
	# 					"type": "plain_text",
	# 					"text": "LinkedIn",
	# 					"emoji": True
	# 				},
	# 				"value": "linkedin",
	# 				"url": "http://www.linkedin.com",
	# 				"action_id": "connect:linkedin"
	# 			}
	# 		]
	# 	}
    # ]
    return interests_header + _create_interests_blocks(user_id, interests) + divider + trending_news_header + one_day_header + trending_day + one_week_header + trending_week

def _create_interests_blocks(user_id, interests):
    interest_button = lambda data: {
		"type": "button",
		"text": {
			"type": "plain_text",
			"text": data
		},
		"value": f"{data}//{user_id}",
		"action_id": f"query_beans:{data})"
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

def get_user_preferences(user_id):
    return userops.get_preferences(_SLACK, user_id)

def update_user_preferences(user_id: str, interests: list[str]):
    userops.update_preferences(_SLACK, user_id, interests)    

_SEARCH_BEANS = "/beans/search"
# _TRENDING_BEANS = "/beans/trending"
# _TRENDING_TOPICS = "/topics/trending"
_TRENDING_NUGGETS = "/nuggets/trending"

def get_beans(nugget: str = None, categories: str|list[str] = None, search_text: str = None, kinds:list[str] = None, window: int = None, limit: int = None):
    params = {}
    if window:
        params["window"]=window,
    if kinds:
        params["kind"]=kinds
    if limit:
        params["topn"]=limit
    
    if nugget:        
        body = {"nuggets": [nugget]}
    elif categories:
        body = {"categories": categories if isinstance(categories, list) else [categories]}
    elif search_text:
        body = {"context": search_text}
    else:
        body = None
        
    # else:         
    #     resp = requests.get(config.get_beansack_url(_TRENDING_BEANS), params=params)
    resp = requests.get(config.get_beansack_url(_SEARCH_BEANS), json=body, params=params)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []

def get_nuggets(categories, window, limit):
    params = {"window": window, "topn": limit}
    body = {"categories": categories if isinstance(categories, list) else [categories]} if categories else None        
    resp = requests.get(config.get_beansack_url(_TRENDING_NUGGETS), params=params, json=body)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []

# def get_topics(window: int = 1):
#     params = {"window": window}
#     resp = requests.get(config.get_beansack_url(_TRENDING_TOPICS), params=params)
#     return resp.json() if (resp.status_code == requests.codes["ok"]) else []