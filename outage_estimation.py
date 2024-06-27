import pandas as pd
from tqdm import tqdm

# Load the data
outage = pd.read_csv('C:/Users/jkim3888/.spyder-py3/outage_all.csv', index_col=False)
 
# Ensure the timestamp column is datetime and drop rows with missing timestamps
outage['timestamp'] = pd.to_datetime(outage['timestamp'], errors='coerce')
outage = outage.dropna(subset=['timestamp'])
 
# Limit the dataset to start from April 13, 2023
start_date_limit = pd.Timestamp('2023-04-13')
outage = outage[outage['timestamp'] >= start_date_limit]

# Sort the DataFrame by zipcode and timestamp
outage = outage.sort_values(by=['zipcode', 'timestamp']) 
# Calculate the span of days for each zip code
date_span = outage.groupby('zipcode')['timestamp'].agg(['min', 'max'])
date_span['span_days'] = (date_span['max'] - date_span['min']).dt.days
 
# Print the date span for each zip code
print("Date span for each zip code:")
print(date_span)
 
# Filter zip codes that span at least 365 days
valid_zipcodes = date_span[date_span['span_days'] >= 365].index
filtered_df = outage[outage['zipcode'].isin(valid_zipcodes)]
 
# Print the valid zip codes
print("Valid zip codes:")
print(valid_zipcodes)

# Print the filtered dataframe head
print("Filtered dataframe:")
print(filtered_df.head())

# Sort the DataFrame by zipcode and timestamp
filtered_df = filtered_df.sort_values(by=['zipcode', 'timestamp'])

outages = []
current_outage = None

# Apply tqdm to the iterrows loop
for index, row in tqdm(filtered_df.iterrows(), total=filtered_df.shape[0], desc="Processing outages"):
    if row['customers_affected'] == 0:
        continue
    
    if current_outage is None:
        current_outage = {
            'zipcode': row['zipcode'],
            'start_time': row['timestamp'],
            'end_time': row['timestamp'],
            'max_customers_affected': row['customers_affected'],
            'total_customers_affected': row['customers_affected'],
            'total_customers_served':row['customers_served']
        }
    else:
        time_diff = (row['timestamp'] - current_outage['end_time']).total_seconds() / 60
        customers_diff = abs(row['customers_affected'] - current_outage['max_customers_affected']) / current_outage['max_customers_affected']
        
        if time_diff <= 16 and customers_diff < 0.2 and row['zipcode'] == current_outage['zipcode']:
            current_outage['end_time'] = row['timestamp']
            current_outage['max_customers_affected'] = max(current_outage['max_customers_affected'], row['customers_affected'])
            current_outage['total_customers_affected'] += row['customers_affected']
            current_outage['total_customers_served'] += row['customers_served']
        else:
            current_outage['duration'] = (current_outage['end_time'] - current_outage['start_time']).total_seconds() / 60 + 15
            current_outage['customers_duration'] = current_outage['max_customers_affected'] * current_outage['duration']
            outages.append(current_outage)
            current_outage = {
                'zipcode': row['zipcode'],
                'start_time': row['timestamp'],
                'end_time': row['timestamp'],
                'max_customers_affected': row['customers_affected'],
                'total_customers_affected': row['customers_affected'],
                'total_customers_served':row['customers_served']
            }

if current_outage:
    current_outage['duration'] = (current_outage['end_time'] - current_outage['start_time']).total_seconds() / 60 + 15
    current_outage['customers_duration'] = current_outage['max_customers_affected'] * current_outage['duration']
    outages.append(current_outage)

outages_df = pd.DataFrame(outages)

# Summarize results by day and zipcode
outages_df['date'] = outages_df['start_time'].dt.date
daily_summary = outages_df.groupby(['date', 'zipcode']).agg({
    'duration': 'sum',
    'customers_duration': 'sum',
    'total_customers_affected': 'sum',
    'max_customers_affected': 'sum',
    'total_customers_served':'mean',
    'zipcode': 'count'  # Number of outages per day per zipcode
}).rename(columns={'zipcode': 'outages_count'})

# Reset the index to make the DataFrame easier to read
daily_summary = daily_summary.reset_index()

# Determine the earliest start date among all zip codes that span >= 365 days
earliest_start_date = daily_summary['date'].min()

# Print the earliest start date
print("Earliest start date:", earliest_start_date)

# Create a complete date range starting from the earliest start date
end_date = daily_summary['date'].max()
date_range = pd.date_range(start=earliest_start_date, end=end_date)

# Ensure all dates are present for each zipcode
zipcodes = daily_summary['zipcode'].unique()
full_index = pd.MultiIndex.from_product([date_range, zipcodes], names=['date', 'zipcode'])
daily_summary = daily_summary.set_index(['date', 'zipcode']).reindex(full_index, fill_value=0).reset_index()

