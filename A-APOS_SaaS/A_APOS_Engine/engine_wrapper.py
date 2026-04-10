import simpy
import numpy as np
import random
from .factory_engine import AdvancedStation, Lot, failure_process

# ── Breakdown 테이블: 구역 → (mttf, mttr) ────────────────────────────────────
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
    for area in BREAKDOWN_TABLE:
        if area.lower().replace("_", "") in station_name.lower().replace("_", ""):
            return area
    prefix_map = {
        "DE_": "Dry_Etch", "WE_": "Wet_Etch", "EPI": "Implant",
        "Implant": "Implant", "Litho_REG": "Litho_Met",
        "LithoMet": "Litho_Met", "LithoTrack": "Litho",
        "Delay": "Dry_Etch",
    }
    for prefix, area in prefix_map.items():
        if station_name.startswith(prefix):
            return area
    return "Dry_Etch"


def _build_route_steps(route_df) -> list:
    """route DataFrame → lot_process용 스텝 리스트 변환"""
    steps = []
    col_map = {c.upper().strip(): c for c in route_df.columns}

    stn_col   = col_map.get("STNFAM")
    ptime_col = col_map.get("MEAN") or col_map.get("PROCTIME") or col_map.get("PT")
    setup_col = col_map.get("SETUP")
    stime_col = col_map.get("SETUP TIME") or col_map.get("STIME")

    if not stn_col or not ptime_col:
        return steps

    for _, row in route_df.iterrows():
        stn = row.get(stn_col)
        if not isinstance(stn, str) or not stn.strip():
            continue
        try:
            ptime = float(row.get(ptime_col) or 10.0)
        except (TypeError, ValueError):
            ptime = 10.0
        try:
            setup_cost = float(row.get(stime_col) or 0.0) if stime_col else 0.0
        except (TypeError, ValueError):
            setup_cost = 0.0

        steps.append({
            "station":    stn.strip(),
            "ptime":      max(0.1, ptime),
            "setup":      str(row.get(setup_col, "")) if setup_col else "",
            "setup_cost": setup_cost,
        })
    return steps


class SimBridge:
    def __init__(self, env: simpy.Environment, data: dict):
        self.env    = env
        self.data   = data
        self.stations: dict[str, AdvancedStation] = {}
        self.active_lots: list    = []
        self.completed_lots: list = []
        self.kpi_tracker = {"completed": 0, "cycle_times": [], "ontime_count": 0}
        self.wip_history: list[dict] = []
        self.kpi_history: list[dict] = []

        # ── 1. Route → 스텝 변환 ─────────────────────────────────────
        self.route_steps: dict[str, list] = {}
        for key, df in data["routes"].items():
            steps = _build_route_steps(df)
            if steps:
                self.route_steps[key] = steps

        # ── 2. 설비 초기화 ───────────────────────────────────────────
        all_stns: dict[str, dict] = {}
        for steps in self.route_steps.values():
            for step in steps:
                stn = step["station"]
                if stn not in all_stns:
                    all_stns[stn] = {"is_batch": False, "min_batch": 0, "capacity": 1}

        # 배치 설비 판별
        for df in data["routes"].values():
            col_map  = {c.upper().strip(): c for c in df.columns}
            bmin_col = col_map.get("BATCH MINIMUM") or col_map.get("BATCHMN")
            stn_col  = col_map.get("STNFAM")
            if not (bmin_col and stn_col):
                continue
            for _, row in df.iterrows():
                stn  = row.get(stn_col)
                bmin = row.get(bmin_col)
                if isinstance(stn, str) and stn.strip() in all_stns:
                    try:
                        bval = float(bmin)
                        if bval > 1:
                            all_stns[stn.strip()]["is_batch"]  = True
                            all_stns[stn.strip()]["min_batch"] = int(bval)
                    except (TypeError, ValueError):
                        pass

        for name, cfg in all_stns.items():
            self.stations[name] = AdvancedStation(
                env, name,
                capacity=cfg["capacity"],
                is_batch=cfg["is_batch"],
                min_batch=cfg["min_batch"],
            )

        # ── 3. 설비 고장 프로세스 (Dataset 3, 4) ─────────────────────
        if data.get("downs") is not None:
            for stn_name, stn_obj in self.stations.items():
                area = _get_area(stn_name)
                mttf, mttr = BREAKDOWN_TABLE.get(area, (10080, 200))
                env.process(failure_process(env, stn_obj, mttf, mttr))

        # ── 4. ★ Lot 투입 프로세스 등록 ★ ──────────────────────────
        orders = data.get("orders")
        if orders is not None and len(orders) > 0 and self.route_steps:
            env.process(self._order_release_process())
        elif self.route_steps:
            env.process(self._dummy_release_process())

    # ── Lot 공정 흐름 ────────────────────────────────────────────────
    def _lot_process(self, lot: Lot, steps: list):
        lot.total_steps  = len(steps)
        lot.current_step = 0

        for step in steps:
            stn_name   = step["station"]
            ptime      = step["ptime"]
            setup_req  = step.get("setup", "")
            setup_cost = step.get("setup_cost", 0.0)

            if stn_name not in self.stations:
                lot.current_step += 1
                continue

            lot.current_station = stn_name
            lot.current_step   += 1
            yield self.env.process(
                self.stations[stn_name].process(lot, ptime, setup_req, setup_cost)
            )

        # 완료
        lot.finish_time     = self.env.now
        lot.current_station = "DONE"
        cycle_time = lot.finish_time - lot.start_time
        is_ontime  = (lot.due_date is None) or (lot.finish_time <= lot.due_date)

        self.kpi_tracker["completed"]    += 1
        self.kpi_tracker["cycle_times"].append(cycle_time)
        self.kpi_tracker["ontime_count"] += int(is_ontime)

        if lot in self.active_lots:
            self.active_lots.remove(lot)
        self.completed_lots.append(lot)

    # ── order.txt 기반 Lot 투입 ──────────────────────────────────────
    def _order_release_process(self):
        orders  = self.data["orders"]
        col_map = {c.upper().strip(): c for c in orders.columns}

        part_col = col_map.get("PART") or col_map.get("PRODUCT NAME") or col_map.get("PRODUCT")
        prio_col = col_map.get("PRIORITY")
        rel_col  = col_map.get("RELEASE_TIME") or col_map.get("START DATE")
        due_col  = col_map.get("DUE_DATE") or col_map.get("DUE DATE")

        lot_counter = 0
        route_keys  = list(self.route_steps.keys())

        for _, row in orders.iterrows():
            part     = str(row.get(part_col, "Product_1")) if part_col else "Product_1"
            priority = int(row.get(prio_col, 10))          if prio_col else 10

            try:
                release_t = float(row.get(rel_col, 0) or 0) if rel_col else 0.0
            except (TypeError, ValueError):
                release_t = 0.0

            try:
                due_date = float(row.get(due_col, 99999) or 99999) if due_col else 99999.0
            except (TypeError, ValueError):
                due_date = 99999.0

            delay = max(0.0, release_t - self.env.now)
            if delay > 0:
                yield self.env.timeout(delay)

            # Route 매핑 시도
            route_key = None
            part_num  = part.split("_")[-1]  # "Product_3" → "3"
            for candidate in [f"part_{part}", f"part_{part_num}", part]:
                if candidate in self.route_steps:
                    route_key = candidate
                    break
            if route_key is None:
                route_key = route_keys[lot_counter % len(route_keys)]

            lot = Lot(
                lot_id    = f"LOT_{lot_counter:05d}",
                part      = part,
                start_time= self.env.now,
                priority  = priority,
                setup_req = "DEFAULT",
                due_date  = due_date,
            )
            lot_counter += 1
            self.active_lots.append(lot)
            self.env.process(self._lot_process(lot, self.route_steps[route_key]))

    # ── 더미 Lot 투입 (order.txt 없을 때) ────────────────────────────
    def _dummy_release_process(self):
        lot_counter = 0
        route_keys  = list(self.route_steps.keys())

        while True:
            yield self.env.timeout(10)

            if len(self.active_lots) >= 200:
                yield self.env.timeout(500)
                continue

            route_key = route_keys[lot_counter % len(route_keys)]
            lot = Lot(
                lot_id    = f"DUMMY_{lot_counter:05d}",
                part      = route_key,
                start_time= self.env.now,
                priority  = random.randint(5, 20),
                setup_req = "DEFAULT",
                due_date  = self.env.now + 50000,
            )
            lot_counter += 1
            self.active_lots.append(lot)
            self.env.process(self._lot_process(lot, self.route_steps[route_key]))

    # ── UI 상태 추출 ─────────────────────────────────────────────────
    def update_ui_state(self) -> dict:
        stn_states = []
        area_stats: dict[str, dict] = {}

        for name, stn in self.stations.items():
            state = stn.state
            area  = _get_area(name)
            stn_states.append({"id": name, "state": state, "util": stn.utilization, "area": area})
            if area not in area_stats:
                area_stats[area] = {"busy": 0, "down": 0, "setup": 0, "idle": 0, "total": 0}
            area_stats[area][state] += 1
            area_stats[area]["total"] += 1

        lot_info = []
        for lot in self.active_lots[:50]:
            cr = 999.0
            if lot.due_date and self.env.now > 0:
                remaining = max(1, lot.total_steps - lot.current_step)
                cr = round((lot.due_date - self.env.now) / remaining, 2)
            lot_info.append({
                "id": lot.id, "part": lot.part,
                "station": lot.current_station or "—",
                "step": lot.current_step, "total": lot.total_steps,
                "cr": cr, "tardy": (lot.due_date is not None and self.env.now > lot.due_date),
                "priority": lot.priority,
            })

        completed  = self.kpi_tracker["completed"]
        cts        = self.kpi_tracker["cycle_times"]
        avg_ct     = round(np.mean(cts), 1) if cts else 0.0
        ontime_pct = round((self.kpi_tracker["ontime_count"] / completed) * 100, 1) if completed > 0 else 0.0
        down_count = sum(1 for s in stn_states if s["state"] == "down")
        wip        = len(self.active_lots)
        tick       = int(self.env.now)

        self.wip_history.append({"tick": tick, "wip": wip})
        self.kpi_history.append({"tick": tick, "ct": avg_ct, "ontime": ontime_pct})
        if len(self.wip_history) > 60: self.wip_history = self.wip_history[-60:]
        if len(self.kpi_history) > 60: self.kpi_history = self.kpi_history[-60:]

        return {
            "tick": tick, "wip": wip,
            "stations": stn_states, "area_stats": area_stats, "lot_info": lot_info,
            "kpi": {"completed": completed, "avg_ct": avg_ct,
                    "ontime_pct": ontime_pct, "down_count": down_count},
            "wip_history": self.wip_history[-30:],
            "kpi_history": self.kpi_history[-30:],
        }

    # ── 시뮬레이션 진행 ──────────────────────────────────────────────
    def run_step(self, until: int) -> dict:
        self.env.run(until=until)
        return self.update_ui_state()

    # ── What-if ──────────────────────────────────────────────────────
    def force_station_down(self, station_name: str, duration: float):
        if station_name in self.stations:
            self.env.process(self._down_process(self.stations[station_name], duration))

    def _down_process(self, stn: AdvancedStation, duration: float):
        stn.is_down = True
        stn.stats["down_time"] += duration
        yield self.env.timeout(duration)
        stn.is_down = False

    def set_lot_priority(self, lot_id: str, new_priority: int) -> bool:
        for lot in self.active_lots:
            if lot.id == lot_id:
                lot.priority = new_priority
                return True
        return False

    def get_summary(self) -> dict:
        total = len(self.stations)
        down  = sum(1 for s in self.stations.values() if s.state == "down")
        busy  = sum(1 for s in self.stations.values() if s.state == "busy")
        return {
            "total_stations": total, "busy": busy, "down": down,
            "idle": total - busy - down,
            "wip": len(self.active_lots),
            "completed": self.kpi_tracker["completed"],
        }