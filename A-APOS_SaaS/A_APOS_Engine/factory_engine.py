import simpy
import random

class Lot:
    def __init__(self, lot_id, part, start_time, priority, setup_req):
        self.id = lot_id
        self.part = part
        self.start_time = start_time
        self.priority = priority
        self.setup_req = setup_req
        self.wait_event = None

class AdvancedStation:
    def __init__(self, env, name, capacity, is_batch=False, min_batch=0):
        self.env = env
        self.name = name
        self.res = simpy.PreemptiveResource(env, capacity=capacity)
        self.current_setup = None
        self.is_batch = is_batch
        self.min_batch = min_batch
        self.batch_queue = []
        self.stats = {"util_time": 0, "setup_time": 0, "down_time": 0}

    def process(self, lot, ptime, setup_req, setup_cost):
        if setup_req and self.current_setup != setup_req:
            self.stats["setup_time"] += setup_cost
            yield self.env.timeout(setup_cost)
            self.current_setup = setup_req

        if self.is_batch:
            self.batch_queue.append(lot)
            lot.wait_event = self.env.event()
            if len(self.batch_queue) >= self.min_batch:
                for l in self.batch_queue:
                    if not l.wait_event.triggered: l.wait_event.succeed()
                self.batch_queue = []
            else:
                yield lot.wait_event

        with self.res.request(priority=1) as req:
            yield req
            start_proc = self.env.now
            try:
                yield self.env.timeout(ptime)
                self.stats["util_time"] += (self.env.now - start_proc)
            except simpy.Interrupt:
                remaining = ptime - (self.env.now - start_proc)
                yield self.env.timeout(remaining)

def failure_process(env, station, mttf, mttr):
    while True:
        yield env.timeout(random.expovariate(1.0 / mttf))
        with station.res.request(priority=-1) as req:
            yield req
            down_duration = random.expovariate(1.0 / mttr)
            station.stats["down_time"] += down_duration
            yield env.timeout(down_duration)
