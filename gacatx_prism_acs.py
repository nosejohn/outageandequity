import pandas as pd
#timestamps = pd.read_csv('C:/Users/jkim3888/Downloads/timestamp_aggregates_zip.csv', index_col = False)
filtered_zipcodes = pd.read_csv('C:/Users/jkim3888/Downloads/filtered_zipcodes_jun182024.csv', index_col = False)
#filtered_zipcodes = filtered_zipcodes.rename(columns = {'ZIP':'zipcode'})

#ga = pd.read_csv('C:/Users/jkim3888/Downloads/combined_ga_zipcode.csv', index_col = False)
#ca = pd.read_csv('C:/Users/jkim3888/Downloads/combined_ca_zipcode.csv', index_col = False)
#tx = pd.read_csv('C:/Users/jkim3888/Downloads/combined_tx_zipcode.csv', index_col = False)

#ga['zipcode'] = ga['zipcode'].astype(str)
#ga['zipcode'] = ga['zipcode'].str.split('.').str[0]
#ca['zipcode'] = ca['zipcode'].astype(str)
#ca['zipcode'] = ca['zipcode'].str.split('.').str[0]
#tx['zipcode'] = tx['zipcode'].astype(str)
#tx['zipcode'] = tx['zipcode'].str.split('.').str[0]

#gacatx_combined = pd.concat([ga, ca, tx], ignore_index = True)


#timestamps_filtered = timestamps[timestamps['time_difference_days']>=365]
#timestamps_filtered = timestamps_filtered[timestamps_filtered['zipcode'].apply(lambda x: isinstance(x, str) and x.isdigit() and len(x) == 5)]


#gacatx_combined_filtered = gacatx_combined[gacatx_combined['zipcode'].isin(timestamps_filtered['zipcode'])]

#gacatx_combined_filtered.groupby(['zipcode', 'utility'])['customers_served'].apply(lambda x: x.isna().sum()).reset_index(name='nan_counts')

from tqdm import tqdm

# Initialize tqdm for pandas
tqdm.pandas()
# Convert timestamp to datetime
#gacatx_combined_filtered['timestamp'] = pd.to_datetime(gacatx_combined_filtered['timestamp'])

# Round timestamp to the nearest 15 minutes
#gacatx_combined_filtered['rounded_timestamp'] = gacatx_combined_filtered['timestamp'].dt.round('15T')

# Ensure numeric type and handle NaNs for summing
#gacatx_combined_filtered['customers_affected'] = pd.to_numeric(gacatx_combined_filtered['customers_affected'], errors='coerce').fillna(0)
#gacatx_combined_filtered['customers_served'] = pd.to_numeric(gacatx_combined_filtered['customers_served'], errors='coerce').fillna(0)

# Group by zipcode and rounded timestamp, then sum customers affected and served
def aggregate_data(df):
    grouped = df.groupby(['zipcode', 'rounded_timestamp']).progress_apply(
        lambda x: pd.Series({
            'customers_affected': x['customers_affected'].sum(),
            'customers_served': x['customers_served'].sum(),
            'state': x['state'].iloc[0]
        })
    )
    return grouped.reset_index()

# Aggregate the data
#aggregated_df = aggregate_data(gacatx_combined_filtered)
aggregated_df = filtered_zipcodes
# Display the aggregated data
print(aggregated_df)
import numpy as np
#aggregated_df['state'] = np.where(aggregated_df['state'].isna(), 'texas', aggregated_df['state'])
import os
import pandas as pd
from collections import defaultdict

# Define the root folder
root_folder_ca = 'C:/Users/jkim3888/Downloads/06'

# Dictionary to hold grouped DataFrames
grouped_dfs_ca = defaultdict(list)


for subdir, _, files in os.walk(root_folder_ca):
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(subdir, file)
            if '_ppt_daily_' in file:
                grouped_dfs_ca['ppt_daily'].append(pd.read_csv(file_path))
            elif '_tmax_daily_' in file:
                grouped_dfs_ca['tmax_daily'].append(pd.read_csv(file_path))
            elif '_tmin_daily_' in file:
                grouped_dfs_ca['tmin_daily'].append(pd.read_csv(file_path))
            elif '_tmean_daily_' in file:
                grouped_dfs_ca['tmean_daily'].append(pd.read_csv(file_path))

# Concatenate DataFrames within each group
ppt_daily_combined_ca = pd.concat(grouped_dfs_ca['ppt_daily'], ignore_index=True)
tmax_daily_combined_ca = pd.concat(grouped_dfs_ca['tmax_daily'], ignore_index=True)
tmin_daily_combined_ca = pd.concat(grouped_dfs_ca['tmin_daily'], ignore_index=True)
tmean_daily_combined_ca = pd.concat(grouped_dfs_ca['tmean_daily'], ignore_index=True)

# Assuming the DataFrames have a common key to merge on, e.g., 'date'
# Merge all the combined DataFrames together
merged_df_ca = pd.merge(ppt_daily_combined_ca, tmax_daily_combined_ca, on=['zcta','date'], how = 'left', suffixes=('_ppt', '_tmax'))
merged_df_ca = pd.merge(merged_df_ca, tmin_daily_combined_ca, on=['zcta','date'], how = 'left', suffixes=('', '_tmin'))
merged_df_ca = pd.merge(merged_df_ca, tmean_daily_combined_ca, on=['zcta','date'], how = 'left', suffixes=('', '_tmean'))

ca_prism = merged_df_ca[['zcta', 'ppt', 'date', 'year_ppt', 'tmax', 'tmin', 'tmean']].rename(columns={'zcta':'zipcode',
                                                                                                  'year_ppt':'year'})


# Define the root folder
root_folder_ga = 'C:/Users/jkim3888/Downloads/13'

# Dictionary to hold grouped DataFrames
grouped_dfs_ga = defaultdict(list)


for subdir, _, files in os.walk(root_folder_ga):
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(subdir, file)
            if '_ppt_daily_' in file:
                grouped_dfs_ga['ppt_daily'].append(pd.read_csv(file_path))
            elif '_tmax_daily_' in file:
                grouped_dfs_ga['tmax_daily'].append(pd.read_csv(file_path))
            elif '_tmin_daily_' in file:
                grouped_dfs_ga['tmin_daily'].append(pd.read_csv(file_path))
            elif '_tmean_daily_' in file:
                grouped_dfs_ga['tmean_daily'].append(pd.read_csv(file_path))

# Concatenate DataFrames within each group
ppt_daily_combined_ga = pd.concat(grouped_dfs_ga['ppt_daily'], ignore_index=True)
tmax_daily_combined_ga = pd.concat(grouped_dfs_ga['tmax_daily'], ignore_index=True)
tmin_daily_combined_ga = pd.concat(grouped_dfs_ga['tmin_daily'], ignore_index=True)
tmean_daily_combined_ga = pd.concat(grouped_dfs_ga['tmean_daily'], ignore_index=True)

# Assuming the DataFrames have a common key to merge on, e.g., 'date'
# Merge all the combined DataFrames together
merged_df_ga = pd.merge(ppt_daily_combined_ga, tmax_daily_combined_ga, on=['zcta','date'],how = 'left', suffixes=('_ppt', '_tmax'))
merged_df_ga = pd.merge(merged_df_ga,tmin_daily_combined_ga, on=['zcta','date'], how = 'left',suffixes=('', '_tmin'))
merged_df_ga = pd.merge(merged_df_ga,tmean_daily_combined_ga, on=['zcta','date'],how = 'left', suffixes=('', '_tmean'))

ga_prism = merged_df_ga[['zcta', 'ppt', 'date', 'year_ppt', 'tmax', 'tmin', 'tmean']].rename(columns={'zcta':'zipcode',
                                                                                                   'year_ppt':'year'})

# Define the root folder
root_folder_tx = 'C:/Users/jkim3888/Downloads/48'

# Dictionary to hold grouped DataFrames
grouped_dfs_tx = defaultdict(list)


for subdir, _, files in os.walk(root_folder_tx):
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(subdir, file)
            if '_ppt_daily_' in file:
                grouped_dfs_tx['ppt_daily'].append(pd.read_csv(file_path))
            elif '_tmax_daily_' in file:
                grouped_dfs_tx['tmax_daily'].append(pd.read_csv(file_path))
            elif '_tmin_daily_' in file:
                grouped_dfs_tx['tmin_daily'].append(pd.read_csv(file_path))
            elif '_tmean_daily_' in file:
                grouped_dfs_tx['tmean_daily'].append(pd.read_csv(file_path))

# Concatenate DataFrames within each group
ppt_daily_combined_tx = pd.concat(grouped_dfs_tx['ppt_daily'], ignore_index=True)
tmax_daily_combined_tx = pd.concat(grouped_dfs_tx['tmax_daily'], ignore_index=True)
tmin_daily_combined_tx = pd.concat(grouped_dfs_tx['tmin_daily'], ignore_index=True)
tmean_daily_combined_tx = pd.concat(grouped_dfs_tx['tmean_daily'], ignore_index=True)

# Assuming the DataFrames have a common key to merge on, e.g., 'date'
# Merge all the combined DataFrames together
merged_df_tx = pd.merge(ppt_daily_combined_tx, tmax_daily_combined_tx, on=['zcta','date'],how = 'left', suffixes=('_ppt', '_tmax'))
merged_df_tx = pd.merge(merged_df_tx,tmin_daily_combined_tx, on=['zcta','date'], how = 'left',suffixes=('', '_tmin'))
merged_df_tx = pd.merge(merged_df_tx,tmean_daily_combined_tx, on=['zcta','date'], how = 'left',suffixes=('', '_tmean'))

tx_prism = merged_df_tx[['zcta', 'ppt', 'date', 'year_ppt', 'tmax', 'tmin', 'tmean']].rename(columns={'zcta':'zipcode',
                                                                                                   'year_ppt':'year'})
prism = pd.concat([ga_prism, ca_prism, tx_prism], ignore_index = True)

prism['date'] = pd.to_datetime(prism['date'], format='%d/%m/%Y')

# Convert the 'timestamp' column to datetime
#aggregated_df = aggregated_df.rename(columns={'rounded_timestamp':'timestamp'})
#aggregated_df['timestamp'] = pd.to_datetime(aggregated_df['timestamp'], format='%m-%d-%Y %H:%M:%S')

# Extract the date and create a new 'date' column
#aggregated_df['date'] = aggregated_df['timestamp'].dt.date

prism['zipcode'] = prism['zipcode'].astype(str).str.zfill(5)

aggregated_df['zipcode'] = aggregated_df['zipcode'].astype(str).str.zfill(5)

aggregated_df['date'] = pd.to_datetime(aggregated_df['date'])
prism['date'] = pd.to_datetime(prism['date'])

gacatx_prism_merged = pd.merge(aggregated_df, prism, on = ['zipcode', 'date'], how = 'left')



acs2021 = pd.read_csv('C:/Users/jkim3888/Downloads/acs2021.csv', index_col = False)

acs2021 = acs2021[['Geo_ZCTA5','SE_A00002_001', 'SE_A00002_002', 'SE_A02001_003', 'SE_A01001B_010', 'SE_A04001_003',
                  'SE_A04001_004', 'SE_A04001_006', 'SE_A04001_010', 'SE_A12001_005',
                  'SE_A14024_001', 'SE_A14028_001', 'SE_A10001_001','SE_A10060_001','SE_A10055_002','SE_A10055_003','SE_A10055_004',
                   'SE_A10055_005', 'SE_A10055_006', 'SE_A10055_007', 'SE_A10055_008', 'SE_A10055_009', 'SE_A10055_010', 'SE_A10055_011',
                  'SE_A10057_001', 'SE_A10034_003']].rename(columns = {'Geo_ZCTA5':'zipcode','SE_A00002_001':'total_population',
                                                                     'SE_A00002_002':'population_density',
                                                                     'SE_A02001_003':'female',
                                                                     'SE_A01001B_010':'age65_older',
                                                                     'SE_A04001_003':'NH_white',
                                                                     'SE_A04001_004':'NH_black',
                                                                     'SE_A04001_006': 'NH_asian',
                                                                     'SE_A04001_010':'hispanic',
                                                                     'SE_A12001_005':'bachelor',
                                                                     'SE_A14024_001':'per_capita_income',
                                                                     'SE_A14028_001':'gini_index',
                                                                     'SE_A10001_001':'housing_units',
                                                                     'SE_A10060_001':'occupied_unites',
                                                                     'SE_A10060_003':'renter_occupied',
                                                                     'SE_A10032_003':'detached_single',
                                                                     'SE_A10055_002':'built_2020_or_later',
                                                                     'SE_A10055_003':'built_2010_2019',
                                                                     'SE_A10055_004':'built_2000_2009',
                                                                     'SE_A10055_005':'built_1990_1999',
                                                                     'SE_A10055_006':'built_1980_1989',
                                                                     'SE_A10055_007':'built_1970_1979',
                                                                     'SE_A10055_008':'built_1960_1969',
                                                                     'SE_A10055_009':'built_1950_1959',
                                                                     'SE_A10055_010':'built_1940_1949',
                                                                     'SE_A10055_011':'built_1939_earlier',
                                                                     'SE_A10057_001':'median_year_structure_built',
                                                                     'SE_A10034_003':'electricity'
})


acs2021['zipcode'] = acs2021['zipcode'].astype(str).str.zfill(5)
acs2021_filtered = acs2021[acs2021['zipcode'].isin(gacatx_prism_merged['zipcode'])]



gacatx_prism_acs_merged = pd.merge(gacatx_prism_merged, acs2021_filtered, on = 'zipcode', how = 'left')
gacatx_prism_acs_merged.head()
