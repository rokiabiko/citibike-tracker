import requests
import pandas as pd
from datetime import datetime
import os
import sys

# URL for real-time station status
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"

def get_status_url():
    """Dynamically find the station_status URL from the GBFS index."""
    try:
        r = requests.get(STATUS_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        for feed in data['data']['en']['feeds']:
            if feed['name'] == 'station_status':
                return feed['url']
    except Exception as e:
        print(f"Error fetching GBFS feed: {e}")
        return None

def main():
    # 1. Get the dynamic URL
    url = get_status_url()
    if not url:
        sys.exit(1) # Fail the action if we can't get the URL

    try:
        # 2. Fetch the actual station data
        print(f"Fetching data from: {url}")
        data = requests.get(url, timeout=10).json()
        stations = data['data']['stations']
        
        # 3. Setup file path with TODAY'S DATE
        fetch_time = datetime.now()
        date_str = fetch_time.strftime("%Y-%m-%d")
        
        # Folder: data/
        # File:   data/2023-10-27_citibike.csv
        output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)
        csv_file = os.path.join(output_dir, f"{date_str}_citibike.csv")

        # 4. Parse the data
        records = []
        for s in stations:
            # Handle E-bikes vs Classic split
            ebikes = 0
            classics = 0
            # Some stations might not list types, so check safely
            if 'vehicle_types_available' in s:
                for v in s['vehicle_types_available']:
                    if v.get('vehicle_type_id') == '1': 
                        classics = v['count']
                    elif v.get('vehicle_type_id') == '2': 
                        ebikes = v['count']
            
            records.append({
                'station_id': s['station_id'],
                'num_bikes': s['num_bikes_available'],
                'num_ebikes': ebikes,
                'num_classics': classics,
                'num_docks': s['num_docks_available'],
                'status': s.get('is_renting', 1),
                'last_reported': s.get('last_reported'),
                'timestamp': fetch_time.strftime("%Y-%m-%d %H:%M:%S")
            })

        df = pd.DataFrame(records)
        
        # 5. Save/Append
        # Check if file exists to determine if we need a header
        header = not os.path.exists(csv_file)
        
        df.to_csv(csv_file, mode='a', header=header, index=False)
        print(f"Successfully logged {len(df)} stations to {csv_file}.")
        
    except Exception as e:
        print(f"Critical Failure: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
