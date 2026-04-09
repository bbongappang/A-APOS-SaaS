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

    def robust_read_csv(self, file_path, sep='\t'):
        for encoding in ['utf-8', 'utf-16', 'utf-16-le', 'latin1', 'cp949']:
            try:
                df = pd.read_csv(file_path, sep=sep, encoding=encoding, on_bad_lines='skip')
                return df
            except:
                continue
        return pd.DataFrame()

    def load_dataset(self, ds_id):
        current_dir = os.getcwd()
        path = os.path.join(current_dir, self.base_path, self.datasets[ds_id])
        
        if not os.path.exists(path):
            raise FileNotFoundError(f"Data path not found: {path}")

        orders = self.robust_read_csv(os.path.join(path, "order.txt"))
        routes = {}
        for file in os.listdir(path):
            if file.startswith("route_") and file.endswith(".txt"):
                part_id = file.split("_")[1].split(".")[0]
                routes[f"part_{part_id}"] = self.robust_read_csv(os.path.join(path, file))
        
        setup_matrix = self.robust_read_csv(os.path.join(path, "setup.txt"))
        downs = self.robust_read_csv(os.path.join(path, "downcal.txt")) if ds_id in [3, 4] else None
        
        # --- Intelligence Extraction ---
        metadata = {
            "total_parts": len(routes),
            "total_orders": len(orders),
            "avg_steps": int(np.mean([len(df) for df in routes.values()])) if routes else 0,
            "bn_candidates": []
        }
        
        # 병목 후보군 추출 (Batch 수량이 100개 이상이거나 Setup 시간이 60분 이상인 설비)
        for df in routes.values():
            if 'STNFAM' in df.columns:
                candidates = df[(df['BATCHMN'] >= 100) | (df['STIME'] >= 60)]['STNFAM'].unique().tolist()
                metadata["bn_candidates"].extend(candidates)
        metadata["bn_candidates"] = list(set(metadata["bn_candidates"]))
            
        return {"orders": orders, "routes": routes, "setup": setup_matrix, "downs": downs, "metadata": metadata}
