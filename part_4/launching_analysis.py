import pandas as pd
import json


# Load JSON data
with open('spacex_launches.json', 'r') as file:
    data = json.load(file)

# Normalize JSON data into a flat table
df = pd.json_normalize(data)

# Convert 'date_utc' to datetime
df['date_utc'] = pd.to_datetime(df['date_utc'])

# Add Launch Year and Launch Month
df['launch_year'] = df['date_utc'].dt.year
df['launch_month'] = df['date_utc'].dt.month

# Add Launch Success as a categorical column
df['launch_success'] = df['success'].apply(lambda x: 'Succsess' if x else 'Failure')
# Display the updated DataFrame
# print(df[['name', 'date_utc', 'launch_year', 'launch_month', 'launch_success']].head())
df.to_csv('launch_data_normalised.csv', index=False)

# Analyze cores

cores_expanded = df.explode('cores').reset_index(drop=True)
cores_data = pd.json_normalize(cores_expanded['cores'])
cores_df = pd.concat([cores_expanded['id'], cores_data], axis=1)
cores_df.rename(columns={'core': 'core_id'}, inplace=True)
# print(cores_df.head())
#How freequently are cores reused?

core_id_counts = cores_df['core_id'].value_counts()
# print(core_id_counts)


#Quick core reuse analysis

# Join core_df with df to get date_utc from df
joined_df = pd.merge(cores_df, df[['id', 'date_utc']], left_on='id', right_on='id', how='left')
cores_df_sorted = joined_df.sort_values(['core_id', 'date_utc'])

cores_df_sorted['previous_day'] = cores_df_sorted.groupby('core_id')['date_utc'].shift(1)
cores_df_sorted['date_diff'] = (cores_df_sorted['date_utc'] - cores_df_sorted['previous_day']).dt.days
cores_df_sorted = cores_df_sorted.sort_values('date_diff', ascending=True).dropna(subset=['date_diff'])

# print(cores_df_sorted.head())


# Busy launch months
# Group by year and month and count the number of launches
launches_per_month = df.groupby(['launch_year', 'launch_month'])['id'].count().reset_index(name='launch_count').sort_values('launch_count', ascending=False)
# launches_per_month.to_csv('launch_count_per_month.csv', index=False)


# Average days between launches
avg_days = cores_df_sorted['date_diff'].mean()
print(f'Average days between launches: {avg_days:.2f} days')

# Assuming 'launchpad' represents the launch site
# Group by launchpad and get counts
success_counts = df[df['launch_success'] == 'Succsess'].groupby('launchpad').size().reset_index(name='successful')
total_counts = df.groupby('launchpad').size().reset_index(name='total')

# Merge the two DataFrames
success_rates = pd.merge(total_counts, success_counts, on='launchpad', how='left').fillna(0)

# Calculate success rate
success_rates['rate'] = (success_rates['successful'] / success_rates['total']) * 100

# Display the DataFrame
