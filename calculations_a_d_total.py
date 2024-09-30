import pandas as pd
import re

# Load the CSV file
file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\test.csv'
df_sorted = pd.read_csv(file_path)

# Convert 'Date' and 'Time' columns to proper datetime formats for calculation
df_sorted['Date'] = pd.to_datetime(df_sorted['Date'], format='%m/%d/%Y', errors='coerce')
df_sorted['Time'] = pd.to_timedelta(df_sorted['Time'], errors='coerce')

# Group by 'Tracking_Number' for efficient processing
grouped_df = df_sorted.groupby('Tracking_Number')

# Initialize the total columns
df_sorted['Total Days/Hour/Minute'] = None
df_sorted['Total Hour/Minute'] = None
df_sorted['Total Hour/Minute Decimal'] = None

# Function to extract days, hours, and minutes from a string
def extract_time_values(time_str):
    days, hours, minutes = 0, 0, 0
    if pd.notnull(time_str):
        # Extract values using regex
        day_match = re.search(r'(\d+)D', time_str)
        hour_match = re.search(r'(\d+)hrs', time_str)
        minute_match = re.search(r'(\d+)m', time_str)
        if day_match:
            days = int(day_match.group(1))
        if hour_match:
            hours = int(hour_match.group(1))
        if minute_match:
            minutes = int(minute_match.group(1))
    return days, hours, minutes

# Iterate over each group (Tracking_Number) and calculate the total times
for tracking_number, group in grouped_df:
    total_days = 0
    total_hours = 0
    total_minutes = 0
    total_decimal_hours = 0.0
    
    # Sum for Days/Hour/Minute format
    for col in ['Label to Arrive Days/Hour/Minute', 'Arrive to Depart Days/Hour/Minute', 'Arrive to Delivery Days/Hour/Minute', 'Label to Delivery Days/Hour/Minute']:
        time_str = group[col].iloc[0]  # Get the time string
        days, hours, minutes = extract_time_values(time_str)  # Extract days, hours, and minutes
        total_days += days
        total_hours += hours
        total_minutes += minutes

    # Correct for overflow minutes into hours
    total_hours += total_minutes // 60
    total_minutes = total_minutes % 60

    # Sum for Hour/Minute format
    total_hour_minutes = 0
    for col in ['Label to Arrive Hour/Minute', 'Arrive to Depart Hour/Minute', 'Arrive to Delivery Hour/Minute', 'Label to Delivery Hour/Minute']:
        time_str = group[col].iloc[0]
        if pd.notnull(time_str):
            hours, minutes = map(int, time_str.split(':'))
            total_hour_minutes += hours * 60 + minutes

    # Sum for Hour/Minute Decimal format
    total_decimal_hours = sum(group[col].iloc[0] for col in ['Label to Arrive Hour/Minute decimal', 'Arrive to Depart Hour/Minute Decimal', 'Arrive to Delivery Hour/Minute Decimal', 'Label to Delivery Hour/Minute Decimal'])

    # Store the results in the first row of the group
    index = group.index[0]
    df_sorted.at[index, 'Total Days/Hour/Minute'] = f'{total_days}D {total_hours:02}hrs {total_minutes:02}m'
    df_sorted.at[index, 'Total Hour/Minute'] = f'{int(total_hour_minutes // 60)}:{int(total_hour_minutes % 60):02}'
    df_sorted.at[index, 'Total Hour/Minute Decimal'] = round(total_decimal_hours, 2)

# Save the modified DataFrame to a new CSV file
output_file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\ups_match_total.csv'
df_sorted.to_csv(output_file_path, index=False)

print(f"Processed file saved as {output_file_path}")




