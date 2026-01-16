import requests
import pandas as pd
from datetime import datetime
import os
import json

# URL for real-time station status
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"
CSV_FILE = "data/citibike_history.csv"

def get_status_url():
    # Helper to resolve the dynamic URL (best practice)
    try:
        r = requests.get(STATUS_URL).json()
        for feed in r['data']['en']['feeds']:
            if feed['name'] == 'station_status':
                return feed['url']
    except:
        return None

def main():
    url = get_status_url()
    if not url: return

    try:
        data = requests.get(url).json()
        stations = data['data']['stations']
        
        # Process the data to flatten it
        records = []
        fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for s in stations:
            # Parse E-bikes vs Classic (if available)
            ebikes = 0
            classics = 0
            if 'vehicle_types_available' in s:
                for v in s['vehicle_types_available']:
                    if v['vehicle_type_id'] == '1': # Usually 1 is classic (check ID mapping if unsure)
                        classics = v['count']
                    elif v['vehicle_type_id'] == '2': # Usually 2 is electric
                        ebikes = v['count']
            
            records.append({
                'station_id': s['station_id'],
                'num_bikes': s['num_bikes_available'],
                'num_ebikes': ebikes,      # NEW COLUMN
                'num_docks': s['num_docks_available'],
                'status': s.get('is_renting', 1),
                'last_reported': s.get('last_reported'), # NEW COLUMN
                'timestamp': fetch_time
            })

        df = pd.DataFrame(records)
        
        # Ensure 'data' folder exists
        os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
        
        # Append to CSV
        header = not os.path.exists(CSV_FILE)
        df.to_csv(CSV_FILE, mode='a', header=header, index=False)
        print(f"Logged {len(df)} stations.")
        
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    main()
