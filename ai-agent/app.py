import streamlit as sl
from agent_core import ask_f1_agent

sl.set_page_config(page_title="AI data analyzer", page_icon="🏎", layout="centered" )
sl.title("F1 telemetry ai analyzer")
sl.caption("-" * 50)
with sl.sidebar:
    sl.title("Examples of promts are shown below:")
    sl.caption("*Ai agent answers on users language*")
    sl.markdown("""
        Show me all data from tyre_performance_age table\n
        \n Who won the race at(track name) on (race year) \n
        \n How (driver`s name) tyres degradated over (num of laps) \n
        \n What was the average soft tyre degradaion over the race in(track name)\n
        
        
        """)
    clear_chat_button = sl.button("Clear chat")
    if clear_chat_button == True:
        sl.session_state.messages
        sl.rerun()
    
    


