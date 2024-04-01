from datetime import datetime
import requests
import config 

_FIRE_MIN = 10

def display_user_data(msg_or_event):
    return [{
			"type": "section",
			"text": {
				"type": "plain_text",
				"text": f"nothing to see here: {msg_or_event['user']}",
				"emoji": True
			}
		}]

def get_trending_items(msg_or_event):
    params = [p.lower() for p in msg_or_event['text'].split(" ")]
    if "beans" in params:
        return get_beans()
    else:
        res = trending_topics()
        return _create_topic_blocks(res[:10])

def get_beans(topics = None, query_texts = None):    
    res = trending_beans(topics = topics, query_texts=query_texts)
    return _create_bean_blocks(res[:5])

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
    return [[
        {
            "type": "actions",
		    "elements": [body(keyword) for keyword in topics]
        }
    ]]

def _create_bean_blocks(beans):
    date_element = lambda data: {
        "type": "plain_text",
        "text": f":date: {datetime.fromtimestamp(data.get('updated')).strftime('%b %d, %Y')}"
    }
    # tags_element = lambda data: {
    #     "type": "plain_text",
    #     "text": f":card_index_dividers: {data.get('tags')[0]}"
    # }
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


_TRENDING_BEANS = "/beans/trending"
_SEARCH_BEANS = "/beans/search"
_TRENDING_TOPICS = "/topics/trending"

def trending_beans(topics = None, query_texts = None):
    if query_texts:
        body = {"query_texts": query_texts}
        resp = requests.get(config.get_beansack_url()+_SEARCH_BEANS, data=body)
    elif topics:
        params = {"topic":topic for topic in topics}
        resp = requests.get(config.get_beansack_url()+_TRENDING_BEANS, params=params)
    else:
        resp = requests.get(config.get_beansack_url()+_TRENDING_BEANS)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []

def trending_topics():
    resp = requests.get(config.get_beansack_url()+_TRENDING_TOPICS)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []