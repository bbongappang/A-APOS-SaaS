import streamlit as st
import streamlit.components.v1 as components
import json
import time
import os
import simpy
from A_APOS_Engine.data_manager import APOSDataManager
from A_APOS_Engine.engine_wrapper import SimBridge

st.set_page_config(layout="wide", page_title="A-APOS Factory OS v2.0", initial_sidebar_state="expanded")

# --- Path Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENGINE_DIR = os.path.join(BASE_DIR, "A_APOS_Engine")

# --- Initialize Session State ---
if 'dm' not in st.session_state:
    st.session_state.dm = APOSDataManager(base_path="SMT_2020 - Final/AutoSched")
    st.session_state.ds_id = 4 
    st.session_state.data = st.session_state.dm.load_dataset(4)
    st.session_state.env = simpy.Environment()
    st.session_state.bridge = SimBridge(st.session_state.env, st.session_state.data)
    st.session_state.tick = 0

# --- Sidebar ---
st.sidebar.title("🚀 A-APOS Control")
ds_choice = st.sidebar.selectbox("Dataset (SMT 2020)", [1, 2, 3, 4], index=st.session_state.ds_id - 1)

if ds_choice != st.session_state.ds_id:
    st.session_state.ds_id = ds_choice
    st.session_state.data = st.session_state.dm.load_dataset(ds_choice)
    st.session_state.env = simpy.Environment()
    st.session_state.bridge = SimBridge(st.session_state.env, st.session_state.data)
    st.session_state.tick = 0
    st.rerun()

sim_speed = st.sidebar.slider("Simulation Step Size", 10, 500, 50)
run_sim = st.sidebar.button("▶ Start AI Optimization Simulation")

# --- Baseline Data ---
baseline_map = {1: 949, 2: 897, 3: 923, 4: 955}

# --- UI Data Preparation ---
current_state = st.session_state.bridge.update_ui_state()
current_state.update({
    "baseline": baseline_map[st.session_state.ds_id],
    "stn_names": sorted(list(st.session_state.bridge.stations.keys())),
    "ds_name": f"Dataset {st.session_state.ds_id}",
    "metadata": st.session_state.data['metadata']
})

# --- Render Dashboard ---
html_path = os.path.join(ENGINE_DIR, "dashboard.html")
if os.path.exists(html_path):
    with open(html_path, "r", encoding="utf-8") as f:
        html_template = f.read()
    
    # Inject Data
    data_injection = f"const realData = {json.dumps(current_state)};"
    final_html = html_template.replace("// [DATA_INJECTION_POINT]", data_injection)
    
    components.html(final_html, height=1500, scrolling=False)
else:
    st.error(f"UI file missing at: {html_path}")

# --- Simulation Loop ---
if run_sim:
    while True:
        st.session_state.tick += sim_speed
        st.session_state.bridge.run_step(until=st.session_state.tick)
        time.sleep(0.05)
        st.rerun()
