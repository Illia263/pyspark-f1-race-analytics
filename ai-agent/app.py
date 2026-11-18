import streamlit as sl
from agent_core import ask_f1_agent

sl.set_page_config(page_title="AI data analyzer", page_icon="🏎", layout="centered" )
sl.title("F1 telemetry AI analyzer")
sl.divider()
with sl.sidebar:
    sl.title("Example prompts:")
    sl.caption("*The AI agent responds in your language*")
    sl.markdown("""
        - Show the top 5 fastest drivers in [track name] [year]
        - Who had the highest average speed at [track name] in [year]?
        - How did [driver's name]'s tyres degrade over [number] laps?
        - What was the average SOFT tyre degradation during the race in [track name]?
        
        
        """)
    clear_chat_button = sl.button("Clear chat")
    if clear_chat_button == True:
        sl.session_state.messages = []
        sl.rerun()
if "messages" not in sl.session_state:
    sl.session_state.messages = [
        {"role": "assistant", "content": "Welcome to AI data analyzer! Asking about what races are loaded in your first query is recommended. Enjoy your time :)"}
    ]

for message in sl.session_state.messages:
    with sl.chat_message(message["role"]):
        sl.markdown(message["content"])

if prompt := sl.chat_input("Ask a question about F1 telemetry ..."):
    sl.session_state.messages.append({"role": "user", "content": prompt})
    with sl.chat_message("user"):
        sl.markdown(prompt)

    with sl.chat_message("assistant"):
        with sl.spinner("Analyzing telemetry & querying PostgreSQL..."):
            try:
                answer = ask_f1_agent(prompt)
                sl.markdown(answer)
                sl.session_state.messages.append(
                    {
                        "role": "assistant", "content": answer,
                        
                     
                    }
                    
                )
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    sl.warning("Rate limit reached (15 req/min). Wait ~40s and try again.")
                else:
                    sl.error(f"Error: {e}")


