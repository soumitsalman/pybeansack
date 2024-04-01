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

def get_beans(msg_or_event):
    params = [p.lower() for p in msg_or_event['text'].split(" ")]
    if "beans" in params:
        res = trending_beans()
        return _create_bean_blocks([item for item in res if item.get('summary')][:5])
    else:
        res = trending_topics()
        return _create_topic_blocks(res[:10])
    
def _create_topic_blocks(topics):
    body = lambda data: {
        "type": "plain_text",
        "text": data.get('keyword') if data.get('Count') < _FIRE_MIN else f"{data.get('keyword')} :fire:",
        "emoji": True
    }
    return [[
        {
            "type": "section",
		    "fields": [body(keyword) for keyword in topics]
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
    # subs_element = lambda data: {
    #     "type": "plain_text",
    #     "text": f":busts_in_silhouette: {data.get('subscribers', 0)}"
    # }

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
            # subs_element(data),                            
            # tags_element(data),
            date_element(data)
        ]
    }
    body = lambda data: {        
		"type": "section",
		"text": {
			"type": "mrkdwn",
			"text": f"[<{data.get('url')}|{data.get('source')}>] *{data.get('title', '')}*\n{data.get('summary')}"
		}
    }
    value = lambda data: f"{data.get('url')}"
    
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
				"value": value(data)
			},
			{
                "action_id": f"negative",
                "type": "button",
				"text": {
					"type": "plain_text",
					"text": ":shit:",
                    "emoji": True
				},
				"value": value(data)
			}
		]
	}
    
    return [[banner(item), body(item), action(item)] for item in beans]


_TRENDING_BEANS = "/beans/trending"
_TRENDING_TOPICS = "/topics/trending"

def trending_beans():
    resp = requests.get(config.get_beansack_url()+_TRENDING_BEANS)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []

def trending_topics():
    resp = requests.get(config.get_beansack_url()+_TRENDING_TOPICS)
    return resp.json() if (resp.status_code == requests.codes["ok"]) else []