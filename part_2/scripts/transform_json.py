import pandas as pd
import json
from typing import List, Dict, Any

def load_json_data(file_path: str) -> List[Dict[str, Any]]:
    """Load JSON data from a file."""
    with open(file_path, 'r') as file:
        return json.load(file)

def create_launches_dataframe(data: List[Dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create the main launches dataframe and details dataframe."""
    df = pd.json_normalize(data)  # Efficiently flatten JSON structure
    df.rename(columns={'id': 'launch_id'}, inplace=True)
    
    # Columns for the main launches table
    launches_cols = [
        'launch_id', 'name', 'date_utc', 'date_unix', 'date_local', 'date_precision',
        'flight_number', 'rocket', 'success', 'launchpad',
        'static_fire_date_utc', 'static_fire_date_unix', 'net', 'window', 'auto_update',
        'tbd', 'launch_library_id', 'upcoming'
    ]
    
    # Split into two DataFrames: launches and details
    launches_df = df[launches_cols]
    details_df = df[['launch_id', 'details']].copy()

    return launches_df, details_df

def process_fairings_and_links(data: List[Dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Process fairings and links data efficiently."""
    fairings = pd.json_normalize(data, 'fairings', ['id'], errors='ignore').rename(columns={'id': 'launch_id'})
    links = pd.json_normalize(data, 'links', ['id'], errors='ignore').rename(columns={'id': 'launch_id'})
    
    # Process 'ships' column in fairings (if exists) efficiently
    if 'ships' in fairings.columns:
        fairings['ships'] = fairings['ships'].apply(lambda x: ','.join(x) if isinstance(x, list) else None)
    
    return fairings, links

def process_cores(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process cores data efficiently using json_normalize."""
    cores_df = pd.json_normalize(data, 'cores', ['id'], errors='ignore').rename(columns={'id': 'launch_id'})
    
    # Filter out rows where 'core' is NaN
    return cores_df[cores_df['core'].notna()]

def process_failures(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process failures data efficiently using json_normalize."""
    failures_df = pd.json_normalize(data, 'failures', ['id'], errors='ignore').rename(columns={'id': 'launch_id'})
    return failures_df

def process_payloads(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process payloads data efficiently."""
    payloads_df = pd.json_normalize(data, 'payloads', ['id'], errors='ignore').rename(columns={'id': 'launch_id'})
    return payloads_df

def main():
    pd.set_option('display.max_columns', None)
    
    # Load data
    data = load_json_data('spacex_launches.json')
    
    # Process dataframes efficiently using vectorized operations
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

if __name__ == "__main__":
    main()