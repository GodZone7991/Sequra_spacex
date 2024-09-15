import pandas as pd
import json
from typing import List, Dict, Any

def load_json_data(file_path: str) -> List[Dict[str, Any]]:
    """Load JSON data from a file."""
    with open(file_path, 'r') as file:
        return json.load(file)

def create_launches_dataframe(data: List[Dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create the main launches dataframe and details dataframe."""
    df = pd.json_normalize(data)
    df.rename(columns={'id': 'launch_id'}, inplace=True)
    
    # Columns for the main launches table (excluding details)
    launches_cols = [
        'launch_id', 'name', 'date_utc', 'date_unix', 'date_local', 'date_precision',
        'flight_number', 'rocket', 'success', 'launchpad',
        'static_fire_date_utc', 'static_fire_date_unix', 'net', 'window', 'auto_update',
        'tbd', 'launch_library_id', 'upcoming'
    ]
    
    # Create the main launches dataframe
    launches_df = df[launches_cols].copy()
    
    # Create a details dataframe with just launch_id and details
    details_df = df[['launch_id', 'details']].copy()

    return launches_df, details_df

def process_fairings_and_links(data: List[Dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Process fairings and links data."""
    fairings_data = []
    links_data = []
    
    for launch in data:
        launch_id = launch['id']
        
        if 'fairings' in launch and launch['fairings']:
            fairings = launch['fairings']
            fairings['launch_id'] = launch_id
            fairings_data.append(fairings)
        
        if 'links' in launch:
            links = launch['links']
            links = {f"{k}_{subk}": subv 
                     for k, v in links.items() 
                     for subk, subv in (v.items() if isinstance(v, dict) else {k: v}.items())}
            links['launch_id'] = launch_id
            links_data.append(links)
    
    fairings_df = pd.DataFrame(fairings_data)
    links_df = pd.DataFrame(links_data)
    
    if 'ships' in fairings_df.columns:
        fairings_df['ships'] = fairings_df['ships'].apply(lambda x: ','.join(x) if isinstance(x, list) else None)
    
    return fairings_df, links_df

def process_cores(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process cores data."""
    cores_data = []
    for launch in data:
        launch_id = launch['id']
        if 'cores' in launch:
            for core in launch['cores']:
                core['launch_id'] = launch_id
                cores_data.append(core)
    
    cores_df = pd.DataFrame(cores_data)
    return cores_df[cores_df['core'].notna()]

def process_failures(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process failures data."""
    failures_data = []
    for launch in data:
        launch_id = launch['id']
        if 'failures' in launch:
            for failure in launch['failures']:
                failure['launch_id'] = launch_id
                failures_data.append(failure)
    
    return pd.DataFrame(failures_data)

def process_payloads(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process payloads data."""
    payloads_data = []
    for launch in data:
        launch_id = launch['id']
        if 'payloads' in launch:
            for payload_id in launch['payloads']:
                payloads_data.append({'launch_id': launch_id, 'payload_id': payload_id})
    
    return pd.DataFrame(payloads_data)

def main():
    pd.set_option('display.max_columns', None)
    
    # Load data
    data = load_json_data('spacex_launches.json')
    
    # Process dataframes
    launches_df, details_df = create_launches_dataframe(data)
    fairings_df, links_df = process_fairings_and_links(data)
    cores_df = process_cores(data)
    failures_df = process_failures(data)
    payloads_df = process_payloads(data)
    
    # Print sample data (replace with your preferred output method)
    print("Launches DataFrame:")
    print(launches_df.head())
    print("\nDetails DataFrame:")
    print(details_df.head())
    print("\nCores DataFrame:")
    print(cores_df.head())
    print("\nFailures DataFrame:")
    print(failures_df.head())
    print("\nPayloads DataFrame:")
    print(payloads_df.head())
    
    # # Uncomment to save to CSV
    # failures_df.to_csv('failures_df.csv', index=False)
    # payloads_df.to_csv('payloads_df.csv', index=False)
    # cores_df.to_csv('cores.csv', index=False)

if __name__ == "__main__":
    main()