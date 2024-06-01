import streamlit as st
from shared import beanops, config, userops, messages
from icecream import ic
import pandas as pd

ESPRESSO = "espresso"

# commands
DIGEST = "/digest"
TRENDING = "/trending"
LOOKFOR = "/lookfor"
KNOWN_COMMANDS = [DIGEST, TRENDING, LOOKFOR]


# session variables
if "messages" not in st.session_state:
    st.session_state.messages = []
if "editors_prefs" not in st.session_state:
    st.session_state.editors_prefs = userops.get_preference_texts(username=userops.EDITOR_USER)
if "editors_select" not in st.session_state:
    # what is the overall highest trending news nugget + then add the editorial
    st.session_state.editors_select = beanops.trending_nuggets_by_topics(userops.get_all_preferences(username=userops.EDITOR_USER), config.DEFAULT_WINDOW, config.MIN_LIMIT)

# callback functions
def update_trends_panel():    
    if st.session_state.topics:
        prefs = userops.get_selected_preferences(st.session_state.topics, username=userops.EDITOR_USER)
        # if there is ONLY 1 topic then load a bunch of items based on limit or else load MIN_LIMIT per topic
        limit = st.session_state.limit if (len(st.session_state.topics) == 1) else config.MIN_LIMIT
        st.session_state.editors_select = beanops.trending_nuggets_by_topics(prefs, st.session_state.window, limit)

def update_trends_panel_for_limit():
    if len(st.session_state.topics) == 1:
        update_trends_panel()
    # else with multiple preferences selected we will show MIN_LIMIT items for each topic so it doesn't matter if limit changes

def create_trends_view(nuggets, window):
    if nuggets:
        # make it into a function
        display_texts = []
        for topic, nuggets in nuggets.items():
            prefix = f":label: **{topic}**: "
            if not nuggets:
                body = messages.NOTHING_TRENDING%window
            elif len(nuggets) == 1:
                body = nuggets[0][config.DESCRIPTION]
            else:
                body = "".join([f"\n- {n[config.DESCRIPTION]}" for n in nuggets])
            display_texts.append(prefix+body)
        return "\n\n".join(display_texts)
    else:
        return messages.NOTHING_TRENDING%window

def create_configuration_view():
    st.subheader("Filters")
    window=st.slider("Last N days", config.MIN_WINDOW, config.MAX_WINDOW, config.DEFAULT_WINDOW, 1, key="window", on_change=update_trends_panel)
    limit=st.slider("Top N items", config.MIN_LIMIT, config.MAX_LIMIT, config.DEFAULT_LIMIT, 1, key="limit", on_change=update_trends_panel_for_limit)
    kinds=st.multiselect("Looking for", config.KINDS, [config.ARTICLE], key="kinds")    
    topics=st.multiselect("Topics", st.session_state.editors_prefs, st.session_state.editors_prefs, key="topics", on_change=update_trends_panel)
    return window, limit, kinds, topics   

def create_message_list_view():
    for msg in st.session_state.messages:
        create_message_view(msg["role"], msg["content"])

def create_message_view(role, content):
    st.chat_message(role, avatar="images/cafecito-ico.ico" if (role==ESPRESSO) else None).markdown(content)

def add_message(role, content):
    create_message_view(role, content)
    st.session_state.messages.append({"role": role, "content": content})

def process_prompt(prompt: str):
    if LOOKFOR in prompt.strip():
        beans = beanops.search_beans(
            search_text=prompt.replace(LOOKFOR, ""),
            kinds=st.session_state.kinds,
            window=st.session_state.window,
            limit=st.session_state.limit
        )
        return f"{len(beans)} beans found" if beans else messages.NOTHING_FOUND
    return messages.NOTHING_FOUND
