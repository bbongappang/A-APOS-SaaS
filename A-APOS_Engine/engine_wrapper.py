import simpy
import json
import time
from A_APOS_Engine.factory_engine import AdvancedStation, Lot, failure_process

class SimBridge:
    """SimPy to UI Bridge"""
    def __init__(self, env, data):
        self.env = env
        self.data = data
        self.stations = {}
        self.active_lots = []
        self.history = {"wip": [], "throughput": 0}
        self.ui_state = {}

        # Initialize Stations from Data
        all_stns = set()
        for route in data['routes'].values():
            all_stns.update(route['STNFAM'].unique())
        
        for stn_name in all_stns:
            # Check if batch stn (Simplified logic)
            is_batch = "Diffusion" in stn_name
            self.stations[stn_name] = AdvancedStation(env, stn_name, capacity=1, is_batch=is_batch, min_batch=5 if is_batch else 0)
            
            # If D3/D4, add failure process
            if data['downs'] is not None:
                # Match stn to failure data (Simplified)
                env.process(failure_process(env, self.stations[stn_name], 10080, 200))

    def update_ui_state(self):
        """Captures current snapshot for JSON/JS consumption"""
        stn_states = []
        down_count = 0
        for name, stn in self.stations.items():
            state = "idle"
            if stn.res.count > 0: state = "busy"
            if stn.stats["setup_time"] > 0: state = "setup" # Simplified
            # if stn.is_down: state = "down"; down_count += 1
            
            stn_states.append({"id": name, "state": state})

        self.ui_state = {
            "tick": int(self.env.now),
            "wip": len(self.active_lots),
            "stations": stn_states,
            "bottlenecks": down_count
        }
        return self.ui_state

    def run_step(self, until):
        self.env.run(until=until)
        return self.update_ui_state()
