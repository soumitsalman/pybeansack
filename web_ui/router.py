import streamlit as st
from . import renderer
from shared import config
from icecream import ic


def load_webui():
    # overall layout
    st.title(config.get_app_name())
    msg_panel, trend_panel = st.columns([0.6, 0.4])
    msg_panel, trend_panel = msg_panel.container(border=True), trend_panel.container(border=True)
    trend_panel.subheader("Trending News")
    # user_panel, editor_panel = trend_panel.tabs(["For You", "Globally"])
    editor_panel = trend_panel.tabs(["Editor's Selects"])[0]

    # side control panel layout
    with st.sidebar:
        window, limit, kinds, topics = renderer.create_configuration_view()

    with editor_panel:    
        # if there are more than 1 item
        st.markdown(renderer.create_trends_view(st.session_state.editors_select, window))

    # message panel layout
    with msg_panel:
        renderer.create_message_list_view()

    # user interaction through chat
    if prompt := st.chat_input("Take a sip"):
        with msg_panel:
            renderer.add_message("user", prompt)                      
            resp = renderer.process_prompt(prompt)
            renderer.add_message(renderer.ESPRESSO, resp) 




