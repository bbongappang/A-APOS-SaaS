import pandas as pd
import numpy as np
import os

class APOSDataManager:
    def __init__(self, base_path="SMT_2020 - Final/AutoSched"):
        self.base_path = base_path
        self.datasets = {
            1: "dataset 1/HVLM_Model/HVLM_Model.asd",
            2: "dataset 2/LVHM_Model/LVHM_Model.asd",
            3: "dataset 3/HVLM_E_Model/HVLM_E_Model.asd",
            4: "dataset 4/LVHM_E_Model/LVHM_E_Model.asd"
        }

    def load_dataset(self, ds_id):
        path = os.path.join(self.base_path, self.datasets[ds_id])
        
        # 1. Order Data
        orders = pd.read_csv(os.path.join(path, "order.txt"), sep='\t')
        
        # 2. Route Data (D1/D3 have route_3,4 / D2/D4 have route_1~10)
        routes = {}
        for file in os.listdir(path):
            if file.startswith("route_") and file.endswith(".txt"):
                part_id = file.split("_")[1].split(".")[0]
                routes[f"part_{part_id}"] = pd.read_csv(os.path.join(path, file), sep='\t')
        
        # 3. Setup Data
        setup_matrix = pd.read_csv(os.path.join(path, "setup.txt"), sep='\t')
        
        # 4. Down/PM (D3, D4 Only)
        downs = None
        if ds_id in [3, 4]:
            downs = pd.read_csv(os.path.join(path, "downcal.txt"), sep='\t')
            
        return {
            "orders": orders,
            "routes": routes,
            "setup": setup_matrix,
            "downs": downs,
            "path": path
        }

    def get_graph_structure(self, ds_id):
        """Future GNN Input: Returns nodes and edges based on routes"""
        data = self.load_dataset(ds_id)
        nodes = []
        edges = []
        
        # Collect all unique stations across all routes
        all_steps = pd.concat(data['routes'].values())
        unique_stns = all_steps['STNFAM'].unique()
        
        for stn in unique_stns:
            nodes.append({"id": stn, "type": "station"})
            
        # Create edges based on sequence in routes
        for part, df in data['routes'].items():
            for i in range(len(df) - 1):
                edges.append({
                    "from": df.iloc[i]['STNFAM'],
                    "to": df.iloc[i+1]['STNFAM'],
                    "part": part
                })
                
        return {"nodes": nodes, "edges": edges}

if __name__ == "__main__":
    dm = APOSDataManager()
    print(f"Dataset 4 Graph Nodes: {len(dm.get_graph_structure(4)['nodes'])}")