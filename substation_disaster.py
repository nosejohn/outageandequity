# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 07:25:54 2024

@author: jkim3888
"""
#C:\Users\jkim3888\Downloads
import pandas as pd
import geopandas as gpd

substation = pd.read_csv('C:/Users/jkim3888/Downloads/2. (2) Electric_Substations.csv', index_col = False)
zipcode = gpd.read_file('C:/Users/jkim3888/Downloads/tl_rd22_us_zcta520.shp', index_col = False)

substation_filtered = substation[(substation['ZIP'] != "NOT AVAILABLE") & (substation['STATUS'] == "IN SERVICE")]

zip_counts = substation_filtered.groupby('ZIP').size().reset_index(name='counts')


zipcode_df = pd.read_csv('C:/Users/jkim3888/Downloads/zipcode_analysis_06232024.csv', index_col = False)

zipcode_df['zipcode'] = zipcode_df['zipcode'].astype(str).str.zfill(5)

zip_counts = zip_counts.rename(columns={'ZIP':'zipcode', 'counts':'substations'})
zip_counts['zipcode']  = zip_counts['zipcode'].astype(str).str.zfill(5)

zip_joined = pd.merge(zipcode_df, zip_counts, on = 'zipcode', how = 'left')
zip_joined['substations'] = zip_joined['substations'].fillna(0)


disaster = pd.read_csv('C:/Users/jkim3888/Downloads/DisasterDeclarationsSummaries (1).csv', index_col = False)

cty_shp = gpd.read_file('C:/Users/jkim3888/Downloads/US_county_2022.shp')

crosswalk = pd.read_csv('C:/Users/jkim3888/Downloads/zip_county_crosswalk.csv', index_col = False)

disaster['fips_code'] = disaster['fipsStateCode'].astype(str).str.zfill(2) + disaster['fipsCountyCode'].astype(str).str.zfill(3)

disaster_filtered = disaster[disaster['incidentType'].isin(['Fire', 'Flood', 'Hurricane', 'Severe Storm', 'Winter Storm',
       'Tornado', 'Snowstorm', 'Earthquake',
       'Mud/Landslide', 'Coastal Storm', 'Other', 'Severe Ice Storm',
       'Dam/Levee Break', 'Tropical Storm', 'Tsunami', 'Typhoon',
       'Volcanic Eruption', 'Freezing'])]

#disaster_filtered['zipcode'] = crosswalk

crosswalk = crosswalk.rename(columns={'ZIP':'zipcode', 'COUNTY':'county'})
crosswalk['zipcode'] = crosswalk['zipcode'].astype(str).str.zfill(5)
crosswalk['county'] = crosswalk['county'].astype(str).str.zfill(5)


zip_joined2 = pd.merge(zip_joined, crosswalk, on='zipcode', how='left')


zip_joined2 = zip_joined2.rename(columns={'county':'fips_code'})

disaster_filtered2 = disaster_filtered[['incidentType', 'incidentBeginDate', 'incidentEndDate', 'fips_code']]

zip_joined3 = pd.merge(zip_joined2, disaster_filtered2, on = 'fips_code', how = 'left')



zip_joined3['date'] = pd.to_datetime(zip_joined3['date'])
zip_joined3['incidentBeginDate'] = pd.to_datetime(zip_joined3['incidentBeginDate'])
zip_joined3['incidentEndDate'] = pd.to_datetime(zip_joined3['incidentEndDate'])

# Aggregate the data by date, zipcode, and incidentType
agg_funcs = {
    'duration': 'first',
    'customers_duration': 'first',
    'total_customers_affected': 'first',
    'max_customers_affected': 'first',
    'outages_count': 'first',
    'num_cus': 'first',
    'ppt': 'first',
    'year': 'first',
    'total_population': 'first',
    'population_density': 'first',
    'female': 'first',
    'age65_older': 'first',
    'nh_white': 'first',
    'nh_black': 'first',
    'nh_asian': 'first',
    'hispanic': 'first',
    'bachelor': 'first',
    'per_capita_income': 'first',
    'gini_index': 'first',
    'housing_units': 'first',
    'occupied_unites': 'first',
    'built_2020_or_later': 'first',
    'built_2010_2019': 'first',
    'built_2000_2009': 'first',
    'built_1990_1999': 'first',
    'built_1980_1989': 'first',
    'built_1970_1979': 'first',
    'built_1960_1969': 'first',
    'built_1950_1959': 'first',
    'built_1940_1949': 'first',
    'built_1939_earlier': 'first',
    'median_year_structure_built': 'first',
    'electricity': 'first',
    'tree': 'first',
    'state': 'first',
    'date_new': 'first',
    'state_encoded': 'first',
    'per_black': 'first',
    'per_hispanic': 'first',
    'per_customers_affected': 'first',
    'per_outage': 'first',
    'per_customer_duration': 'first',
    'per_female': 'first',
    'per_pop_customers_duration': 'first',
    'per_bachelor': 'first',
    'per_old': 'first',
    'ln_per_customer_duration': 'first',
    'per_black_per_capita_income': 'first',
    'per_hispanic_per_capita_income': 'first',
    'per_black_tmax': 'first',
    'per_hispanic_tmax': 'first',
    'per_black_median_year_structure': 'first',
    'per_white': 'first',
    'per_non_white': 'first',
    'median_hhi': 'first',
    'total_poverty': 'first',
    'per_poverty': 'first',
    'substations': 'first',
    'fips_code': 'first',
}

# Aggregate the data by date, zipcode, incidentType, and incidentBeginDate
grouped = zip_joined3.groupby(['date', 'zipcode', 'incidentType', 'incidentBeginDate']).agg(agg_funcs).reset_index()

# Count unique incident types per date, zipcode, and incidentBeginDate
incident_counts = zip_joined3.groupby(['date', 'zipcode', 'incidentType', 'incidentBeginDate']).size().reset_index(name='incident_count')

# Merge the counts back into the grouped data
grouped = pd.merge(grouped, incident_counts, on=['date', 'zipcode', 'incidentType', 'incidentBeginDate'], how='left')

# Pivot the table to create columns for each incident type showing sums of each incident
pivoted = grouped.pivot_table(index=['date', 'zipcode'], columns='incidentType', values='incident_count', aggfunc='sum', fill_value=0).reset_index()

# Merge the pivoted table with the grouped data
final_df = pd.merge(grouped, pivoted, on=['date', 'zipcode'], how='left')

# Display the final DataFrame
print(final_df)


# Ensure ZIP codes in crosswalk_df are 5-digit strings
crosswalk['zipcode'] = crosswalk['zipcode'].astype(str).str.zfill(5)

# Merge with the crosswalk DataFrame
final_df = pd.merge(final_df, crosswalk, on='zipcode', how='left')
