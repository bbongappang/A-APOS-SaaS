import simpy
import numpy as np
from .factory_engine import AdvancedStation, failure_process

# ── Breakdown 데이터: 구역명 → (mttf, mttr) 매핑 ──────────────────────────────
BREAKDOWN_TABLE = {
    "Def_Met":    (10080, 35.28),
    "Dielectric": (10080, 604.8),
    "Diffusion":  (10080, 151.2),
    "Dry_Etch":   (10080, 231.84),
    "Implant":    (10080, 604.8),
    "Litho":      (10080, 705.59),
    "Litho_Met":  (10080, 35.28),
    "Planar":     (10080, 201.6),
    "TF":         (10080, 453.6),
    "TF_Met":     (10080, 35.28),
    "Wet_Etch":   (10080, 221.76),
}

def _get_area(station_name: str) -> str:
    """설비명에서 공정 구역(Area)을 추출"""
    for area in BREAKDOWN_TABLE:
        if area.lower().replace("_", "") in station_name.lower().replace("_", ""):
            return area
    # 접두사 매칭
    prefix_map = {
        "DE_": "Dry_Etch", "WE_": "Wet_Etch", "EPI": "Implant",
        "Implant": "Implant", "Litho_REG": "Litho_Met",
        "LithoMet": "Litho_Met", "LithoTrack": "Litho",
        "Delay": "Dry_Etch",
    }
    for prefix, area in prefix_map.items():
        if station_name.startswith(prefix):
            return area
    return "Dry_Etch"  # 기본값


class SimBridge:
    def __init__(self, env: simpy.Environment, data: dict):
        self.env = env
        self.data = data
        self.stations: dict[str, AdvancedStation] = {}
        self.active_lots: list = []
        self.completed_lots: list = []

        # KPI 히스토리 (대시보드 차트에 실제 데이터 전달)
        self.kpi_tracker = {
            "completed": 0,
            "cycle_times": [],       # 완료 Lot의 Cycle Time 목록
            "ontime_count": 0,       # 납기 내 완료 Lot 수
        }
        self.wip_history: list[dict] = []   # {"tick": t, "wip": n}
        self.kpi_history: list[dict] = []   # {"tick": t, "ct": avg, "ontime": %}

        # ── 설비 초기화 ──────────────────────────────────────────────
        all_stations: dict[str, dict] = {}   # name → {is_batch, min_batch, capacity}

        for route_df in data["routes"].values():
            if "STNFAM" not in route_df.columns:
                continue
            for _, row in route_df.iterrows():
                stn = row.get("STNFAM")
                if not isinstance(stn, str):
                    continue
                if stn not in all_stations:
                    all_stations[stn] = {
                        "is_batch": False,
                        "min_batch": 0,
                        "capacity": 1,
                    }
                # 배치 설비 판별: BATCH MINIMUM 컬럼 활용
                bmin = row.get("BATCH MINIMUM") or row.get("BATCHMN")
                if bmin and not np.isnan(float(bmin)) and float(bmin) > 1:
                    all_stations[stn]["is_batch"] = True
                    all_stations[stn]["min_batch"] = int(float(bmin))

        # AdvancedStation 객체 생성
        for name, cfg in all_stations.items():
            self.stations[name] = AdvancedStation(
                env,
                name,
                capacity=cfg["capacity"],
                is_batch=cfg["is_batch"],
                min_batch=cfg["min_batch"],
            )

        # ── 설비 고장 프로세스 (Dataset 3, 4만) ──────────────────────
        if data.get("downs") is not None:
            for stn_name, stn_obj in self.stations.items():
                area = _get_area(stn_name)
                mttf, mttr = BREAKDOWN_TABLE.get(area, (10080, 200))
                env.process(failure_process(env, stn_obj, mttf, mttr))

    # ── UI 상태 추출 ──────────────────────────────────────────────────
    def update_ui_state(self) -> dict:
        """대시보드에 전달할 전체 상태 JSON 생성"""

        # 1. 설비 상태
        stn_states = []
        area_stats: dict[str, dict] = {}

        for name, stn in self.stations.items():
            state = stn.state          # .state 프로퍼티 사용 (정확한 우선순위)
            area  = _get_area(name)

            stn_states.append({
                "id":    name,
                "state": state,
                "util":  stn.utilization,
                "area":  area,
            })

            if area not in area_stats:
                area_stats[area] = {"busy": 0, "down": 0, "setup": 0, "idle": 0, "total": 0}
            area_stats[area][state] += 1
            area_stats[area]["total"] += 1

        # 2. Lot 추적 정보
        lot_info = []
        for lot in self.active_lots[:50]:   # 최대 50개만 전달
            cr = 999.0
            if lot.due_date and self.env.now > 0:
                remaining_steps = max(1, lot.total_steps - lot.current_step)
                remaining_time  = lot.due_date - self.env.now
                cr = round(remaining_time / remaining_steps, 2)
            lot_info.append({
                "id":       lot.id,
                "part":     lot.part,
                "station":  lot.current_station or "—",
                "step":     lot.current_step,
                "total":    lot.total_steps,
                "cr":       cr,
                "tardy":    (lot.due_date is not None and self.env.now > lot.due_date),
                "priority": lot.priority,
            })

        # 3. KPI 계산
        completed = self.kpi_tracker["completed"]
        cts = self.kpi_tracker["cycle_times"]
        avg_ct  = round(np.mean(cts), 1) if cts else 0
        ontime_pct = round((self.kpi_tracker["ontime_count"] / completed) * 100, 1) if completed > 0 else 0

        down_count = sum(1 for s in stn_states if s["state"] == "down")
        wip = len(self.active_lots)

        # 4. 히스토리 기록 (매 호출마다 스냅샷)
        tick = int(self.env.now)
        self.wip_history.append({"tick": tick, "wip": wip})
        self.kpi_history.append({"tick": tick, "ct": avg_ct, "ontime": ontime_pct})
        # 최근 60개만 보관
        if len(self.wip_history) > 60:
            self.wip_history = self.wip_history[-60:]
        if len(self.kpi_history) > 60:
            self.kpi_history = self.kpi_history[-60:]

        return {
            "tick":         tick,
            "wip":          wip,
            "stations":     stn_states,
            "area_stats":   area_stats,
            "lot_info":     lot_info,
            "kpi": {
                "completed":  completed,
                "avg_ct":     avg_ct,
                "ontime_pct": ontime_pct,
                "down_count": down_count,
            },
            "wip_history":  self.wip_history[-30:],
            "kpi_history":  self.kpi_history[-30:],
        }

    # ── 시뮬레이션 진행 ───────────────────────────────────────────────
    def run_step(self, until: int) -> dict:
        self.env.run(until=until)
        return self.update_ui_state()

    # ── What-if: 설비 강제 다운 ──────────────────────────────────────
    def force_station_down(self, station_name: str, duration: float):
        """UI 슬라이더로 특정 설비를 강제로 다운시킴"""
        if station_name in self.stations:
            stn = self.stations[station_name]
            self.env.process(self._down_process(stn, duration))

    def _down_process(self, stn: AdvancedStation, duration: float):
        stn.is_down = True
        stn.stats["down_time"] += duration
        yield self.env.timeout(duration)
        stn.is_down = False

    # ── What-if: 우선순위 변경 (GNN 연동 포인트) ─────────────────────
    def set_lot_priority(self, lot_id: str, new_priority: int):
        """GNN 에이전트가 Lot 우선순위를 변경할 때 호출"""
        for lot in self.active_lots:
            if lot.id == lot_id:
                lot.priority = new_priority
                return True
        return False

    # ── 상태 요약 (사이드바 메트릭용) ────────────────────────────────
    def get_summary(self) -> dict:
        total = len(self.stations)
        down  = sum(1 for s in self.stations.values() if s.state == "down")
        busy  = sum(1 for s in self.stations.values() if s.state == "busy")
        return {
            "total_stations": total,
            "busy":  busy,
            "down":  down,
            "idle":  total - busy - down,
            "wip":   len(self.active_lots),
            "completed": self.kpi_tracker["completed"],
        }