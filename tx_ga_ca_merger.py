import os
import glob
import pandas as pd

# Base directory path
base_path = '/Users/johnkim/Documents/study/gatech/research/outage/equity/txgaca/ga'

# List of layout directories to process
layouts = [1, 2, 3, 4, 5, 7, 9, 10, 11]

# Iterate over each layout directory
for layout in layouts:
    layout_path = os.path.join(base_path, f'layout_{layout}')
    file_pattern = os.path.join(layout_path, 'per_*.csv')
    
    # Use glob to find all files matching the pattern in the current layout directory
    csv_files = glob.glob(file_pattern)
    
    # Initialize a list to hold the DataFrames for the current layout
    layout_df_list = []
    
    # Loop through the list of files and read each one into a DataFrame
    for file in csv_files:
        df = pd.read_csv(file)
        layout_df_list.append(df)
    
    # Concatenate all DataFrames for the current layout
    if layout_df_list:
        layout_combined_df = pd.concat(layout_df_list, ignore_index=True)
        
        # Save the combined DataFrame for the current layout
        output_path = os.path.join(base_path, f'combined_outages_layout_{layout}.csv')
        layout_combined_df.to_csv(output_path, index=False)
        print(f"Saved combined DataFrame for layout {layout} to {output_path}")


base_path = '/Users/johnkim/Documents/study/gatech/research/outage/equity/txgaca/ga'

# List of layout directories to process
layouts = [1, 2, 3, 4, 5, 7, 9, 10, 11]

# Dictionary to hold the combined DataFrames
combined_datasets = {}

# Load each combined dataset from the saved CSV files
for layout in layouts:
    file_path = os.path.join(base_path, f'combined_outages_layout_{layout}.csv')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        combined_datasets[layout] = df
        print(f"Loaded combined dataset for layout {layout}")

# Example: Access the combined DataFrame for a specific layout
print(combined_datasets[1].head())


combined_datasets[1] = combined_datasets[1].rename(columns = {'outageRecID':'id', 'zip': 'zipcode', 'customersOutNow':'customers_affected',
                                       'EMC': 'utility'})

combined_datasets[1] = combined_datasets[1][['id', 'zipcode', 'timestamp', 'customers_affected', 'utility']]

combined_dataset_1 = combined_datasets[1]

import geopandas as gpd
from shapely.geometry import Point
zip_shp = gpd.read_file('/Users/johnkim/US_zcta_2020.shp')

import ast
from tqdm import tqdm

# Ensure 'OutageLocation' is parsed correctly with tqdm progress bar
tqdm.pandas(desc="Parsing OutageLocation")
combined_datasets[2]['OutageLocation'] = combined_datasets[2]['OutageLocation'].progress_apply(safe_parse)

# Remove rows where parsing failed
combined_datasets[2] = combined_datasets[2].dropna(subset=['OutageLocation'])

# Convert outage coordinates to a GeoDataFrame with tqdm progress bar
tqdm.pandas(desc="Converting to Geometry")
combined_datasets[2]['geometry'] = combined_datasets[2]['OutageLocation'].progress_apply(lambda loc: Point(loc['X'], loc['Y']))

# Create the GeoDataFrame with the geometry column
combined_gdf = gpd.GeoDataFrame(combined_datasets[2], geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
combined_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

# Perform the spatial join
outage_with_zip = gpd.sjoin(combined_gdf, zip_shp, how='left', op='within')

# Display the resulting DataFrame
print(outage_with_zip.head())

combined_dataset_2 = outage_with_zip[['OutageRecID', 'ZCTA5CE20', 'timestamp', 'CustomersOutNow', 'CustomersServed',
                                     'EMC']]

combined_dataset_2 = combined_dataset_2.rename(columns = {'OutageRecID': 'id', 'ZCTA5CE20':'zipcode',
                                                          'CustomersOutNow':'customers_affected',
                                                          'CustomersServed':'customers_served','EMC':'utility'})

zip_shp = zip_shp[['ZCTA5CE20', 'geometry']]

zip_shp = zip_shp.rename(columns = {'ZCTA5CE20' : 'zipcode'})

tqdm.pandas(desc="Converting to Geometry")
combined_datasets[3]['geometry'] = combined_datasets[3].progress_apply(lambda row: Point(row['X'], row['Y']), axis=1)

# Create the GeoDataFrame with the geometry column
outage_gdf = gpd.GeoDataFrame(combined_datasets[3], geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
outage_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

# Perform the spatial join
outage_with_zip = gpd.sjoin(outage_gdf, zip_shp, how='left', op='within')


combined_dataset_3 = outage_with_zip
combined_dataset_3 = combined_dataset_3[['CaseNumber','zipcode','timestamp', 'CutomersAffected', 'EMC']]
combined_dataset_3 = combined_dataset_3.rename(columns ={'CaseNumber':'id','CutomersAffected':'customers_affected',
                                                         'EMC':'utility'})

# Extract ZIP code from the 'name' column
combined_datasets[4]['zipcode'] = combined_datasets[4]['name'].str.extract(r'(\d{5})')

print(combined_datasets[4])

combined_dataset_4 = combined_datasets[4]

combined_dataset_4 = combined_dataset_4[['zipcode', 'timestamp', 'cust_a', 'cust_s', 'EMC']]

combined_dataset_4 = combined_dataset_4.rename(columns = {'cust_a': 'customers_affected', 
                                                          'cust_s': 'customers_served', 'EMC':'utility'})

combined_dataset_1.to_csv('combined_dataset_ga1.csv', index = False)
combined_dataset_2.to_csv('combined_dataset_ga2.csv', index = False)
combined_dataset_3.to_csv('combined_dataset_ga3.csv', index = False)

# Create GeoDataFrame from outage data
tqdm.pandas(desc="Converting coordinates to geometries")
combined_datasets[5]['geometry'] = combined_datasets[5].progress_apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
outage_gdf = gpd.GeoDataFrame(combined_datasets[5], geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
outage_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

# Perform the spatial join to find ZIP codes
outage_with_zip = gpd.sjoin(outage_gdf, zip_shp, how='left', op='within')


combined_dataset_5 = outage_with_zip
combined_dataset_5 = combined_dataset_5

combined_dataset_5 = combined_dataset_5[['id', 'zipcode', 'timestamp', 'numPeople', 'EMC']].rename(columns={
    'numPeople': 'customers_affected',
    'EMC': 'utility'
})

# Create GeoDataFrame from outage data
tqdm.pandas(desc="Converting coordinates to geometries")
combined_datasets[7]['geometry'] = combined_datasets[7].progress_apply(lambda row: Point(row['lon'], row['lat']), axis=1)
outage_gdf = gpd.GeoDataFrame(combined_datasets[7], geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
outage_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

outage_with_zip = gpd.sjoin(outage_gdf, zip_shp, how='left', op='within')


outage_with_zip['substation'].nunique()
# greystone provides substation level data.... a lot of lon and lat are missing (as well as zip codes)


combined_dataset_9 = combined_datasets[9]

combined_dataset_9 = combined_dataset_9[['Zip Code', '# Out', '# Served', 'timestamp', 'EMC']].rename(columns = {'Zip Code':'zipcode', 
                                                                                                                 '# Out': 'customers_affected',
                                                                                                                 '# Served':'customers_served',
                                                                                                                 'EMC':'utility'})

tqdm.pandas(desc="Converting coordinates to geometries")
combined_datasets[11]['geometry'] = combined_datasets[11].progress_apply(lambda row: Point(row['lon'], row['lat']), axis=1)
outage_gdf = gpd.GeoDataFrame(combined_datasets[11], geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
outage_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

# Perform the spatial join to find ZIP codes
outage_with_zip = gpd.sjoin(outage_gdf, zip_shp, how='left', op='within')
print(outage_with_zip)


combined_dataset_11 = outage_with_zip

combined_dataset_11 = combined_dataset_11[['incident_id','timestamp', 'consumers_affected', 'zipcode', 'EMC']].rename(columns = {
  'incident_id':'id', 'consumers_affected':'customers_affected', 'EMC':'utility'  
})

combined_ga = pd.concat([combined_dataset_1, combined_dataset_2, combined_dataset_3, combined_dataset_4, combined_dataset_5,
         combined_dataset_9, combined_dataset_11], ignore_index = True)

print(combined_ga)


combined_ga = combined_ga[['zipcode', 'utility', 'timestamp', 'customers_affected', 'customers_served', 'id']]
combined_ga.to_csv('combined_ga_zipcode.csv', index = False)

# Base directory path
base_path = '/Users/johnkim/Documents/study/gatech/research/outage/equity/txgaca/tx'

# List of layout directories to process
layouts = [1, 4, 6, 8, 10, 11, 12, 13, 16, 17, 18, 19]

# Iterate over each layout directory
for layout in layouts:
    layout_path = os.path.join(base_path, f'layout_{layout}')
    file_pattern = os.path.join(layout_path, 'per_*.csv')
    
    # Use glob to find all files matching the pattern in the current layout directory
    csv_files = glob.glob(file_pattern)
    
    # Initialize a list to hold the DataFrames for the current layout
    layout_df_list = []
    
    # Loop through the list of files and read each one into a DataFrame
    for file in csv_files:
        df = pd.read_csv(file)
        layout_df_list.append(df)
    
    # Concatenate all DataFrames for the current layout
    if layout_df_list:
        layout_combined_df = pd.concat(layout_df_list, ignore_index=True)
        
        # Save the combined DataFrame for the current layout
        output_path = os.path.join(base_path, f'combined_outages_layout_{layout}.csv')
        layout_combined_df.to_csv(output_path, index=False)
        print(f"Saved combined DataFrame for layout {layout} to {output_path}")


base_path = '/Users/johnkim/Documents/study/gatech/research/outage/equity/txgaca/tx'

# List of layout directories to process
layouts = [1, 4, 6, 8, 10, 11, 12, 13, 16, 17, 18, 19]

# Dictionary to hold the combined DataFrames
combined_datasets = {}

# Load each combined dataset from the saved CSV files
for layout in layouts:
    file_path = os.path.join(base_path, f'combined_outages_layout_{layout}.csv')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        combined_datasets[layout] = df
        print(f"Loaded combined dataset for layout {layout}")

# Example: Access the combined DataFrame for a specific layout
print(combined_datasets[1].head())

combined_dataset_1 = combined_datasets[1]
combined_dataset_1 = combined_dataset_1[['ZIP', 'CUSTOMER OUTAGES', 'CUSTOMERS SERVED',
                                        'timestamp', 'EMC']].rename(columns = {'ZIP':'zipcode',
                                                                                             'CUSTOMER OUTAGES':'customers_affected',
                                                                                             'CUSTOMERS SERVED':'customers_served',
                                                                               'EMC':'utility'})

combined_dataset_1
#texas layout1 has values 'fewer than 5' in their customers affected column... also zip code is not regularly recorded...
# I am displacing any rows with non-numeric values in zip code...
combined_dataset_1['zipcode'] = pd.to_numeric(combined_dataset_1['zipcode'], errors = 'coerce')
combined_dataset_1 = combined_dataset_1.dropna(subset=['zipcode'])

combined_dataset_1['zipcode'] = combined_dataset_1['zipcode'].astype(int)

combined_dataset_1
combined_dataset_1['zipcode'] = combined_dataset_1['zipcode'].astype(str).str.zfill(5)

# if Fewer than 5, = 3!
combined_dataset_1.loc[combined_dataset_1['customers_affected'] == "Fewer than 5", "customers_affected"] = 3

tqdm.pandas(desc="Converting coordinates to geometries")
combined_datasets[4]['geometry'] = combined_datasets[4].progress_apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
outage_gdf = gpd.GeoDataFrame(combined_datasets[4], geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
outage_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

# Perform the spatial join to find ZIP codes
outage_with_zip = gpd.sjoin(outage_gdf, zip_shp, how='left', op='within')
print(outage_with_zip)

combined_dataset_4 = combined_dataset_4[combined_dataset_4['state'] == "T"]

combined_dataset_4 = combined_dataset_4[['zipcode', 'customersServed', 'customersAffected',
                                        'timestamp', 'EMC']].rename(columns = {
    'customersServed':'customers_served', 'customersAffected':'customers_affected', 'EMC':'utility'
})

tqdm.pandas(desc="Converting coordinates to geometries")
combined_datasets[6]['geometry'] = combined_datasets[6].progress_apply(lambda row: Point(row['x'], row['y']), axis=1)
outage_gdf = gpd.GeoDataFrame(combined_datasets[6], geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
outage_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

# Perform the spatial join to find ZIP codes
outage_with_zip = gpd.sjoin(outage_gdf, zip_shp, how='left', op='within')
print(outage_with_zip)

combined_dataset_6 = combined_datasets[6]

combined_dataset_8 = combined_datasets[8]
combined_dataset_8 = combined_dataset_8[['outageRecID', 'customersOutNow', 'timestamp', 'zip', 'EMC']].rename(
columns = {'outageRecID':'id', 'customersOutNow':'customers_affected', 'zip':'zipcode', 'EMC':'utility'})

combined_dataset_10 = combined_datasets[10]


combined_dataset_10 = combined_dataset_10[['id', 'custAffected', 'zip', 'timestamp', 'EMC']].rename(columns={
    'custAffected':'customers_affected', 'zip':'zipcode', 'EMC':'utility'
})

combined_datasets_11 = combined_datasets[11]
print(combined_datasets_11)

# Combine 'Zip Code' and 'Zip Codes' into 'zipcode'
combined_datasets_11['zipcode'] =combined_datasets_11['Zip Code'].combine_first(combined_datasets_11['Zip Codes'])

# Drop the original columns if no longer needed
combined_datasets_11.drop(columns=['Zip Code', 'Zip Codes'], inplace=True)

# Display the updated DataFrame
print(combined_datasets_11)

combined_datasets_11 = combined_datasets_11[['zipcode','# Out', '# Served', 'timestamp', 'EMC']].rename(columns={
    '# Out':'customers_affected', '# Served':'customers_served', 'EMC':'utility'
})

combined_datasets[12]
# 12 has duration as well...
combined_dataset_12 = combined_datasets[12]
combined_dataset_12

combined_dataset_12 = combined_dataset_12[['zip_code','incident_id', 'consumers_affected', 'NumConsumers', 'timestamp',
                                          'EMC']]

combined_dataset_12 = combined_dataset_12.rename(columns={'zip_code':'zipcode', 'incident_id':'id', 'consumers_affected':'customers_affected',
                                                         'NumConsumers':'customers_served','EMC':'utility'})

combined_dataset_16 = combined_datasets[16]
combined_dataset_16


tqdm.pandas(desc="Converting coordinates to geometries")
combined_datasets[16]['geometry'] = combined_datasets[16].progress_apply(lambda row: Point(row['X'], row['Y']), axis=1)
outage_gdf = gpd.GeoDataFrame(combined_datasets[16], geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
outage_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

# Perform the spatial join to find ZIP codes
outage_with_zip = gpd.sjoin(outage_gdf, zip_shp, how='left', op='within')
print(outage_with_zip)

combined_dataset_16 = outage_with_zip
combined_dataset_16

combined_dataset_16 = combined_dataset_16[['zipcode','CaseNumber', 'CutomersAffected', 'timestamp', 'EMC']].rename(columns={
    'CaseNumber':'id', 'CutomersAffected':'customers_affected', 'EMC':'utility'
})

combined_dataset_18 = combined_datasets[18]
combined_dataset_18 = combined_dataset_18[combined_dataset_18["summaryField"]=="ZIP"]

sum(combined_dataset_18['summaryFieldValue']=="Undefined")
# 23216 are undefined out of 78249


combined_dataset_19 = combined_datasets[19]
combined_dataset_19 = combined_dataset_19[['name', 'cust_a', 'cust_s', 'timestamp', 'EMC']].rename(columns={
    'name':'zipcode', 'cust_a':'customers_affected', 'cust_s':'customers_served', 'EMC':'utility'
})

combined_tx = pd.concat([combined_dataset_1, combined_dataset_4, combined_dataset_5, combined_dataset_8,
         combined_dataset_10, combined_dataset_11, combined_dataset_12, combined_dataset_16,
                        combined_dataset_19], ignore_index = True)
# excluding 6 due to lat, long issue
print(combined_tx)

combined_tx.to_csv("combined_tx_zipcode.csv", index = False)

# Set the directory path
directory_path = '/Users/johnkim/Documents/study/gatech/research/outage/equity/txgaca/ca/layout_investor'

# List to hold data from each CSV file
data_frames = []

# Iterate through all files in the directory
for file_name in os.listdir(directory_path):
    # Check if the file is a CSV file
    if file_name.endswith('.csv'):
        # Create the full file path
        file_path = os.path.join(directory_path, file_name)
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        # Append the DataFrame to the list
        data_frames.append(df)

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(data_frames, ignore_index=True)

combined_df = combined_df.drop_duplicates(subset=['ImpactedCustomers', 'x', 'y', 'timestamp'])


tqdm.pandas(desc="Converting coordinates to geometries")
combined_df['geometry'] = combined_df.progress_apply(lambda row: Point(row['x'], row['y']), axis=1)
outage_gdf = gpd.GeoDataFrame(combined_df, geometry='geometry')

# Ensure both GeoDataFrames use the same coordinate reference system (CRS)
outage_gdf.set_crs(epsg=4326, inplace=True)
zip_shp.to_crs(epsg=4326, inplace=True)

# Perform the spatial join to find ZIP codes
outage_with_zip = gpd.sjoin(outage_gdf, zip_shp, how='left', op='within')
print(outage_with_zip)

combined_ca = outage_with_zip
combined_ca = combined_ca[['GlobalID','zipcode', 'UtilityCompany','timestamp', 'ImpactedCustomers']]
combined_ca = combined_ca.rename(columns={'GlobalID':'id', 'UtilityCompany':'utility', 'ImpactedCustomers':'customers_affected'})
combined_ca['state'] = 'california'

# Set the directory path
directory_path = '/Users/johnkim/Documents/study/gatech/research/outage/equity/txgaca/ca/layout_paloalto'

# List to hold data from each CSV file
data_frames = []

# Iterate through all files in the directory
for file_name in os.listdir(directory_path):
    # Check if the file is a CSV file
    if file_name.endswith('.csv'):
        # Create the full file path
        file_path = os.path.join(directory_path, file_name)
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        # Append the DataFrame to the list
        data_frames.append(df)

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(data_frames, ignore_index=True)

# Combine 'Zip Code' and 'Zip Codes' into 'zipcode'
combined_df['zipcode'] =combined_df['zip'].combine_first(combined_df['Zip Code'])
combined_df['customers_affected'] =combined_df['# Out'].combine_first(combined_df['customersOutNow'])
# Drop the original columns if no longer needed
combined_df.drop(columns=['Zip Code', 'zip'], inplace=True)
combined_df.drop(columns=['# Out', 'customersOutNow'], inplace=True)



combined_df = combined_df[['outageRecID','zipcode','EMC','customers_affected', '# Served', 'timestamp']]

combined_df = combined_df.rename(columns={'outageRecID':'id', 'EMC':'utility', '# Served':'customers_served'})

combined_df['state'] = 'california'

california_outage = pd.concat([combined_df, combined_ca], ignore_index=True)

california_outage.to_csv('combined_ca_zipcode.csv', index = False)

texas_outage = pd.read_csv('/Users/johnkim/combined_tx_zipcode.csv', index_col = False)

texas_outage['state'] = 'texas'

georgia_outage = pd.read_csv('/Users/johnkim/combined_ga_zipcode.csv', index_col = False)

georgia_outage['state'] = 'georgia'

georgia_outage.to_csv('combined_ga_zipcode.csv', index = False)
texas_outage.to_csv('combined_tx_zipcode.csv', index = False)
