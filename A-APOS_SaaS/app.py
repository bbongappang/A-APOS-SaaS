import streamlit as st
import streamlit.components.v1 as components
import json
import time
import os
import simpy
from A_APOS_Engine.data_manager import APOSDataManager
from A_APOS_Engine.engine_wrapper import SimBridge

st.set_page_config(
    layout="wide",
    page_title="A-APOS Factory OS v2.0",
    initial_sidebar_state="expanded"
)

# ── 경로 설정 ────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
ENGINE_DIR = os.path.join(BASE_DIR, "A_APOS_Engine")

# ── Baseline 리드타임 (데이터셋별) ───────────────────────────────────────────
BASELINE_MAP = {1: 949, 2: 897, 3: 923, 4: 955}

DS_LABELS = {
    1: "DS1 · HVLM (소품종 대량)",
    2: "DS2 · LVHM (다품종 소량)",
    3: "DS3 · HVLM_E (고장 포함)",
    4: "DS4 · LVHM_E (고장 포함)",
}

# ── Session State 초기화 ─────────────────────────────────────────────────────
def init_session(ds_id: int):
    dm   = st.session_state.get("dm") or APOSDataManager(base_path="SMT_2020 - Final/AutoSched")
    data = dm.load_dataset(ds_id)
    env  = simpy.Environment()
    bridge = SimBridge(env, data)

    st.session_state.dm         = dm
    st.session_state.ds_id      = ds_id
    st.session_state.data       = data
    st.session_state.env        = env
    st.session_state.bridge     = bridge
    st.session_state.tick       = 0
    st.session_state.running    = False
    st.session_state.kpi_log    = []    # {"tick", "wip", "ct", "ontime", "down"}

if "dm" not in st.session_state:
    init_session(4)

# ── 사이드바 ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🚀 A-APOS Control")
    st.divider()

    # 1. Dataset 선택
    st.subheader("📂 Dataset")
    ds_choice = st.selectbox(
        "SMT 2020 모델 선택",
        [1, 2, 3, 4],
        index=st.session_state.ds_id - 1,
        format_func=lambda x: DS_LABELS[x],
    )
    if ds_choice != st.session_state.ds_id:
        init_session(ds_choice)
        st.rerun()

    st.divider()

    # 2. 시뮬레이션 제어
    st.subheader("⚙️ Simulation Control")
    sim_speed = st.slider("Step Size (분)", 10, 500, 50, step=10,
                          help="한 번 진행할 시뮬레이션 시간(분). 작을수록 세밀, 클수록 빠름")

    col1, col2 = st.columns(2)
    with col1:
        run_btn  = st.button("▶ 시작", use_container_width=True, type="primary")
    with col2:
        stop_btn = st.button("⏹ 중단", use_container_width=True)
    reset_btn = st.button("🔄 초기화", use_container_width=True)

    if run_btn:
        st.session_state.running = True
    if stop_btn:
        st.session_state.running = False
    if reset_btn:
        init_session(st.session_state.ds_id)
        st.rerun()

    st.divider()

    # 3. 실시간 KPI (사이드바)
    st.subheader("📊 실시간 KPI")
    summary = st.session_state.bridge.get_summary()

    st.metric("Sim Time (Tick)", f"T+{st.session_state.tick}")
    st.metric("WIP (재공품)", f"{summary['wip']:,} lots")
    st.metric("완료 Lot", f"{summary['completed']:,} lots")

    kh = st.session_state.bridge.kpi_history
    if kh:
        last = kh[-1]
        st.metric("평균 Cycle Time", f"{last['ct']:.0f} h")
        st.metric("납기 준수율", f"{last['ontime']:.1f} %")

    c1, c2, c3 = st.columns(3)
    c1.metric("🟦 Busy",  summary["busy"])
    c2.metric("🔴 Down",  summary["down"])
    c3.metric("⬛ Idle",  summary["idle"])

    st.divider()

    # 4. What-if Controller
    st.subheader("🎛️ What-if Controller")
    st.caption("특정 설비를 강제로 다운시켜 AI 회복력을 테스트합니다")

    stn_names = sorted(list(st.session_state.bridge.stations.keys()))
    forced_stn = st.selectbox("설비 선택", ["(없음)"] + stn_names)
    forced_dur = st.slider("강제 다운 시간 (분)", 30, 1440, 120, step=30)
    if st.button("⚠️ 강제 다운 적용", use_container_width=True):
        if forced_stn != "(없음)":
            st.session_state.bridge.force_station_down(forced_stn, forced_dur)
            st.warning(f"{forced_stn} → {forced_dur}분 다운 예약됨")

    st.divider()
    st.caption(f"A-APOS v2.0 · Dataset {st.session_state.ds_id}\nBaseline LT: {BASELINE_MAP[st.session_state.ds_id]}h")

# ── UI 데이터 준비 ────────────────────────────────────────────────────────────
current_state = st.session_state.bridge.update_ui_state()
current_state.update({
    "baseline":  BASELINE_MAP[st.session_state.ds_id],
    "stn_names": stn_names,
    "ds_name":   DS_LABELS[st.session_state.ds_id],
    "metadata":  st.session_state.data["metadata"],
    # 실제 breakdown 데이터를 대시보드로 전달
    "breakdown": [
        {"area": "Def_Met",    "mttf": 10080, "mttr": 35.28},
        {"area": "Dielectric", "mttf": 10080, "mttr": 604.8},
        {"area": "Diffusion",  "mttf": 10080, "mttr": 151.2},
        {"area": "Dry_Etch",   "mttf": 10080, "mttr": 231.84},
        {"area": "Implant",    "mttf": 10080, "mttr": 604.8},
        {"area": "Litho",      "mttf": 10080, "mttr": 705.59},
        {"area": "Litho_Met",  "mttf": 10080, "mttr": 35.28},
        {"area": "Planar",     "mttf": 10080, "mttr": 201.6},
        {"area": "TF",         "mttf": 10080, "mttr": 453.6},
        {"area": "TF_Met",     "mttf": 10080, "mttr": 35.28},
        {"area": "Wet_Etch",   "mttf": 10080, "mttr": 221.76},
    ],
    # KPI 히스토리 (차트용 실제 데이터)
    "wip_history": current_state.get("wip_history", []),
    "kpi_history": current_state.get("kpi_history", []),
})

# KPI 로그 누적
st.session_state.kpi_log.append({
    "tick":   st.session_state.tick,
    "wip":    current_state["wip"],
    "ct":     current_state["kpi"]["avg_ct"],
    "ontime": current_state["kpi"]["ontime_pct"],
    "down":   current_state["kpi"]["down_count"],
})

# ── 대시보드 렌더링 ───────────────────────────────────────────────────────────
html_path = os.path.join(ENGINE_DIR, "dashboard.html")
if os.path.exists(html_path):
    with open(html_path, "r", encoding="utf-8") as f:
        html_template = f.read()

    data_injection = f"const realData = {json.dumps(current_state, ensure_ascii=False)};"
    final_html = html_template.replace("// [DATA_INJECTION_POINT]", data_injection)

    components.html(final_html, height=1500, scrolling=False)
else:
    st.error(f"❌ dashboard.html 파일 없음: {html_path}")
    st.info("A_APOS_Engine/ 폴더 안에 dashboard.html을 넣어주세요.")

# ── 시뮬레이션 루프 ───────────────────────────────────────────────────────────
if st.session_state.running:
    st.session_state.tick += sim_speed
    st.session_state.bridge.run_step(until=st.session_state.tick)
    time.sleep(0.1)   # CPU 과부하 방지
    st.rerun()