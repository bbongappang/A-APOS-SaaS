import simpy
from .factory_engine import AdvancedStation, Lot, failure_process

class SimBridge:
    def __init__(self, env, data):
        self.env = env
        self.data = data
        self.stations = {}
        self.active_lots = []
        
        all_stns = set()
        for route in data['routes'].values():
            # Filter out NaN or non-string values from STNFAM column
            valid_stns = [s for s in route['STNFAM'].unique() if isinstance(s, str)]
            all_stns.update(valid_stns)
        
        for name in all_stns:
            # Now we are sure name is a string
            is_batch = "Diffusion" in name
            self.stations[name] = AdvancedStation(env, name, capacity=1, is_batch=is_batch, min_batch=5 if is_batch else 0)
            
            # Failure process for D3/D4
            if data['downs'] is not None:
                # Find matching failure data (Simplified matching)
                env.process(failure_process(env, self.stations[name], 10080, 200))

    def update_ui_state(self):
        stn_states = []
        for name, stn in self.stations.items():
            state = "idle"
            if stn.res.count > 0: state = "busy"
            if stn.is_batch and len(stn.batch_queue) > 0: state = "setup"
            stn_states.append({"id": name, "state": state})
        
        return {
            "tick": int(self.env.now),
            "wip": len(self.active_lots),
            "stations": stn_states
        }

    def run_step(self, until):
        self.env.run(until=until)
        return self.update_ui_state()
