import streamlit as st
import streamlit.components.v1 as components
import json
import time
from A_APOS_Engine.data_manager import APOSDataManager
from A_APOS_Engine.engine_wrapper import SimBridge
import simpy

st.set_page_config(layout="wide", page_title="A-APOS Live Brain")

# 1. Initialize Data & Sim
if 'dm' not in st.session_state:
    st.session_state.dm = APOSDataManager()
    st.session_state.ds_id = 4 # Default to DS4
    st.session_state.data = st.session_state.dm.load_dataset(st.session_state.ds_id)
    st.session_state.env = simpy.Environment()
    st.session_state.bridge = SimBridge(st.session_state.env, st.session_state.data)
    st.session_state.tick = 0

# 2. UI Layout
st.sidebar.title("A-APOS Control")
ds_choice = st.sidebar.selectbox("Select Dataset", [1, 2, 3, 4], index=3)
if ds_choice != st.session_state.ds_id:
    st.session_state.ds_id = ds_choice
    st.session_state.data = st.session_state.dm.load_dataset(ds_choice)
    st.session_state.env = simpy.Environment()
    st.session_state.bridge = SimBridge(st.session_state.env, st.session_state.data)
    st.rerun()

sim_speed = st.sidebar.slider("Sim Speed (Step Size)", 1, 100, 10)
run_sim = st.sidebar.button("Run / Resume AI Simulation")

# 3. HTML/JS Dashboard (The code provided by Claude)
# We wrap the HTML code in a string and inject the live state via postMessage
with open("A_APOS_Engine/dashboard.html", "r", encoding="utf-8") as f:
    html_code = f.read()

# Inject the current state into the HTML/JS via a custom script at the end
current_state_json = json.dumps(st.session_state.bridge.update_ui_state())
html_with_data = html_code.replace("// [DATA_INJECTION_POINT]", f"const initialState = {current_state_json};")

components.html(html_with_data, height=800, scrolling=False)

# 4. Sim Loop
if run_sim:
    placeholder = st.empty()
    while True:
        st.session_state.tick += sim_speed
        state = st.session_state.bridge.run_step(until=st.session_state.tick)
        
        # In a real app, we'd use a more efficient way to push data to the JS component
        # For this MVP, we re-render or use a trigger
        time.sleep(0.1)
        st.rerun()
