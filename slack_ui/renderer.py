from itertools import chain
from shared.config import *
from shared import espressops, beanops, messages
from icecream import ic
import logging
from datetime import datetime as dt

DEFAULT_KIND = (NEWS, BLOG)
UPDATE_INTEREST_VIEW={
    "type": "modal",
    "callback_id": "new_interest_input",
    "title": {"type": "plain_text", "text": "Espresso by Cafecit.io"},
    "submit": {"type": "plain_text", "text": "Update"},
    "blocks": [
        {
            "block_id": "new_interest_input",
            "type": "input",
            "element": {
                "type": "plain_text_input",
                "action_id": "new_interests"
            },
            "label": {
                "type": "plain_text",
                "text": "Your Interests (comma separated)"
            }
        }
    ]
}
DIVIDER = {"type": "divider"}

logging.basicConfig(format="[%(asctime)s]: %(levelname)s - %(message)s",  datefmt='%d/%b/%Y %H:%M:%S')
render_date = lambda date: dt.fromtimestamp(date).strftime('%a, %b %d')

def render_home_blocks(settings):   
    blocks = render_settings(settings)
    blocks.append(DIVIDER)

    categories = tuple(settings['search']['topics'])
    tags = beanops.trending_tags(categories, None, MIN_WINDOW, DEFAULT_LIMIT)    
    if tags:
        blocks.append(render_text_banner("Trending Tags", True))
        blocks.append(render_tags([tag.tags for tag in tags]))
        blocks.append(DIVIDER)
    
    beans = beanops.trending(None, categories, None, DEFAULT_KIND, MIN_WINDOW, 0, MAX_ITEMS_PER_PAGE)
    if beans:
        blocks.append(render_text_banner("Trending News & Articles", True))    
        blocks.extend(chain(*(render_bean_digest(bean) for bean in beans)))
        blocks.append(DIVIDER)

    blocks.extend(render_connections(settings))    
    return blocks

def render_connections(settings):
    return [
        render_text_banner("Accounts", True),
        {
            "type": "actions",
            "elements": [
                render_button("delete-account", "Delete Account", style="danger") if "user" in settings else render_button("register-account", "Sign Up", style="primary")
            ]
        }
    ] 

def render_settings(settings):
    return [
        render_text_banner("Topics of Interest", True, False),
        {
            "type": "actions",
            "elements": [render_button("category", cat, label = espressops.category_label(cat)) for cat in settings['search']['topics']]
        },
        {
            "type": "actions",
            "elements": [
                render_button("update", "Update Interests", style = "primary")
            ]
        }
    ]

def render_tags(tags: list[str]):
    return {
        "type": "actions",
        "elements": [render_button("tag", tag) for tag in tags]
    }
    
def render_button(action_type, value, label = None, style = None): 
    button = {        
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": label or value,
            "emoji": True
        },
        "value": f"{value}",
        "action_id": f"{action_type}:{value}"
    }
    if style:
        button["style"] = style
    return button

def render_text_banner(text: str, as_header: bool, image_url: str = None):
    banner = {
        "type": "header" if as_header else "section",
        "text": {
            "type": "plain_text" if as_header else "mrkdwn",
            "text": text             
        }
    }
    if image_url:
        banner["accessory"] = {
            "type": "image",
            "image_url": image_url,
            "alt_text": text
        }
    return banner

def render_bean_digest(bean: Bean):
    banner_text = f"{render_date(bean.created or bean.updated)}: *<{bean.url}|{bean.title}>*"    
    context = ""       
    if bean.comments:
        context += f"  💬 {bean.comments}"
    if bean.likes:
        context += f"  👍 {bean.likes}"
    if context:
        banner_text += "  -"+context
    return [render_text_banner(banner_text, False, None)]

def render_whole_bean(bean: Bean):
    context = [
        {
            "type": "plain_text",
            "text": render_date(bean.created or bean.updated)
        },
        {
            "type": "image",
            "image_url": beanops.favicon(bean),
            "alt_text": bean.source
        },
        {
            "type": "plain_text",
            "text": bean.source,
        }
    ]        
    if bean.comments:
        context.append({
            "type": "plain_text",
            "text": f"💬 {bean.comments}"
        })
    if bean.likes:
        context.append({
            "type": "plain_text",
            "text": f"👍 {bean.likes}"
        })    
    return [
        {"type": "context", "elements": context},
        render_text_banner(f"*<{bean.url}|{bean.title}>*", False, bean.image_url),
        render_text_banner(bean.summary, False, None),
        DIVIDER
    ] 

# def get_digests(username, search_text: str):
#     # this should search across the board without window
#     result = llmops._create_digest(search_text, DEFAULT_WINDOW, DEFAULT_LIMIT)  
#     return [_create_digest_blocks(nug, res, beans) for nug, res, beans in result] if result else messages.NOTHING_FOUND

# def _create_digest_blocks(nugget, summary, beans): 
#     # sources = {bean['source']: f"<{bean['url']}|{bean['source']}>" for bean in beans}
#     return render_text_banner([
#         f":rolled_up_newspaper: *{nugget[KEYPHRASE]}: {nugget[DESCRIPTION]}*", 
#         summary,
#         "*:link: * "+", ".join({bean['source']: f"<{bean['url']}|{bean['source']}>" for bean in beans}.values())
#     ])

# def update_user_preferences(user_id: str, interests: list[str]):
#     espressops.update_topics(source=config.SLACK, username=user_id, items=interests)    
 