import simpy
import random

class Lot:
    def __init__(self, lot_id, part, start_time, priority, setup_req, due_date=None):
        self.id = lot_id
        self.part = part
        self.start_time = start_time
        self.priority = priority
        self.setup_req = setup_req
        self.due_date = due_date          # 납기일 (분 단위)
        self.current_step = 0             # 현재 공정 단계
        self.total_steps = 0              # 전체 공정 단계 수
        self.current_station = None       # 현재 위치한 설비명
        self.wait_start = None            # 대기 시작 시간 (Queue Time 계산용)
        self.finish_time = None           # 완료 시각
        self.wait_event = None

    @property
    def critical_ratio(self):
        """CR = 잔여 납기 시간 / 잔여 공정 단계 수 (낮을수록 긴급)"""
        if self.due_date is None or self.total_steps == 0:
            return 999.0
        remaining_steps = max(1, self.total_steps - self.current_step)
        remaining_time = self.due_date - self._env_now
        return remaining_time / remaining_steps

    @property
    def is_tardy(self):
        """납기 초과 여부"""
        if self.due_date is None:
            return False
        return self._env_now > self.due_date

    # env.now를 외부에서 주입받기 위한 setter
    def set_env(self, env):
        self._env_now = env.now
        return self


class AdvancedStation:
    def __init__(self, env, name, capacity=1, is_batch=False, min_batch=0):
        self.env = env
        self.name = name
        self.capacity = capacity
        self.res = simpy.PreemptiveResource(env, capacity=capacity)
        self.current_setup = None
        self.is_batch = is_batch
        self.min_batch = min_batch
        self.batch_queue = []
        self.is_down = False              # ← 고장 상태 플래그 추가
        self.stats = {
            "util_time": 0,
            "setup_time": 0,
            "down_time": 0,
            "lots_processed": 0,
            "total_wait_time": 0,
        }

    @property
    def state(self):
        """설비 상태를 우선순위에 따라 정확하게 반환"""
        if self.is_down:
            return "down"
        if self.res.count > 0:
            return "busy"
        if self.is_batch and len(self.batch_queue) > 0:
            return "setup"
        return "idle"

    @property
    def utilization(self):
        """가동률 (%) — util_time / env.now"""
        if self.env.now == 0:
            return 0.0
        return round((self.stats["util_time"] / self.env.now) * 100, 1)

    def process(self, lot, ptime, setup_req, setup_cost):
        # 1. 셋업 변경
        if setup_req and self.current_setup != setup_req:
            self.stats["setup_time"] += setup_cost
            yield self.env.timeout(setup_cost)
            self.current_setup = setup_req

        # 2. 배치 대기
        if self.is_batch:
            self.batch_queue.append(lot)
            lot.wait_event = self.env.event()
            if len(self.batch_queue) >= self.min_batch:
                for l in self.batch_queue:
                    if not l.wait_event.triggered:
                        l.wait_event.succeed()
                self.batch_queue = []
            else:
                yield lot.wait_event

        # 3. 설비 점유 및 처리
        with self.res.request(priority=lot.priority) as req:
            wait_start = self.env.now
            yield req
            self.stats["total_wait_time"] += (self.env.now - wait_start)

            start_proc = self.env.now
            lot.current_station = self.name
            try:
                yield self.env.timeout(ptime)
                self.stats["util_time"] += (self.env.now - start_proc)
                self.stats["lots_processed"] += 1
            except simpy.Interrupt:
                remaining = ptime - (self.env.now - start_proc)
                yield self.env.timeout(remaining)
                self.stats["util_time"] += ptime


def failure_process(env, station, mttf, mttr):
    """설비 고장 프로세스 — is_down 플래그 반영"""
    while True:
        # 고장까지 대기
        yield env.timeout(random.expovariate(1.0 / mttf))

        station.is_down = True
        down_duration = random.expovariate(1.0 / mttr)

        with station.res.request(priority=-999) as req:
            yield req
            station.stats["down_time"] += down_duration
            yield env.timeout(down_duration)

        station.is_down = False


def lot_process(env, lot, route_steps, stations, completed_lots, kpi_tracker):
    """
    Lot이 route_steps 순서대로 설비를 거치는 전체 공정 프로세스.

    route_steps: [
        {"station": "Litho_FE_98", "ptime": 45.0, "setup": "TYPE_A", "setup_cost": 10.0},
        ...
    ]
    """
    lot.total_steps = len(route_steps)
    lot.current_step = 0

    for step in route_steps:
        stn_name = step["station"]
        ptime    = step["ptime"]
        setup_req = step.get("setup")
        setup_cost = step.get("setup_cost", 0.0)

        if stn_name not in stations:
            continue

        stn = stations[stn_name]
        lot.current_station = stn_name
        lot.current_step += 1

        yield env.process(stn.process(lot, ptime, setup_req, setup_cost))

    # 완료 처리
    lot.finish_time = env.now
    lot.current_station = "DONE"
    completed_lots.append(lot)

    # KPI 기록
    cycle_time = lot.finish_time - lot.start_time
    is_ontime = (lot.due_date is None) or (lot.finish_time <= lot.due_date)
    kpi_tracker["completed"] += 1
    kpi_tracker["cycle_times"].append(cycle_time)
    kpi_tracker["ontime_count"] += int(is_ontime)


def lot_release_process(env, orders_df, routes_dict, stations,
                        active_lots, completed_lots, kpi_tracker):
    """
    order.txt 기반으로 Lot을 시뮬레이션 시간에 맞춰 투입하는 프로세스.
    orders_df 컬럼: PART, PRIORITY, RELEASE_TIME(분), DUE_DATE(분)
    """
    lot_counter = 0

    for _, row in orders_df.iterrows():
        part      = str(row.get("PART", "unknown"))
        priority  = int(row.get("PRIORITY", 10))
        release_t = float(row.get("RELEASE_TIME", 0))
        due_date  = float(row.get("DUE_DATE", 99999))
        setup_req = str(row.get("SETUP", "DEFAULT"))

        # 투입 시각까지 대기
        delay = max(0, release_t - env.now)
        if delay > 0:
            yield env.timeout(delay)

        lot = Lot(
            lot_id=f"LOT_{lot_counter:04d}",
            part=part,
            start_time=env.now,
            priority=priority,
            setup_req=setup_req,
            due_date=due_date,
        )
        lot_counter += 1

        route_key = f"part_{part}"
        if route_key not in routes_dict:
            continue

        route_steps = routes_dict[route_key]
        active_lots.append(lot)

        env.process(
            lot_process(env, lot, route_steps, stations,
                        completed_lots, kpi_tracker)
        )

        # 완료된 Lot은 active에서 제거
        for done in completed_lots:
            if done in active_lots:
                active_lots.remove(done)