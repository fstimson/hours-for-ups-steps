import pandas as pd

# Load the sorted CSV file from the path
file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\hours_days.csv'
df_sorted = pd.read_csv(file_path)

# Remove "0 days" from Time column and ensure proper format
df_sorted['Time'] = pd.to_timedelta(df_sorted['Time'].str.replace('0 days ', '', regex=False))

# Ensure the 'Date' column is in the correct format (mm/dd/yyyy)
df_sorted['Date'] = pd.to_datetime(df_sorted['Date'], format='%m/%d/%Y', errors='coerce')

# Create new columns for the calculations
df_sorted['Label to Arrive Days/Hour/Minute'] = None
df_sorted['Label to Arrive Hour/Minute'] = None
df_sorted['Label to Arrive Hour/Minute decimal'] = None  # New column for decimal format

# Group by 'Tracking_Number' for efficient processing
grouped_df = df_sorted.groupby('Tracking_Number')

# Iterate over each group (Tracking_Number) and calculate the differences
for tracking_number, group in grouped_df:
    # Ensure the group has both "Label_Created" and "Arrived_at_Facility" events
    if 'Label_Created' in group['Class'].values and 'Arrived_at_Facility' in group['Class'].values:
        # Get the 'Label_Created' event
        label_created_row = group[group['Class'] == 'Label_Created'].iloc[0]
        label_created_datetime = label_created_row['Date'] + label_created_row['Time']

        # Get the first 'Arrived_at_Facility' event after 'Label_Created'
        arrived_at_facility_row = group[group['Class'] == 'Arrived_at_Facility'].iloc[0]
        arrived_at_facility_datetime = arrived_at_facility_row['Date'] + arrived_at_facility_row['Time']

        # Calculate the time difference
        time_diff = arrived_at_facility_datetime - label_created_datetime

        # Days, hours, and minutes
        days = time_diff.days
        hours, remainder = divmod(time_diff.seconds, 3600)
        minutes, _ = divmod(remainder, 60)

        # Format Days/Hour/Minute compactly (if 0 days, omit it)
        time_parts = []
        if days > 0:
            time_parts.append(f"{days}d")
        if hours > 0:
            time_parts.append(f"{hours}hrs")
        if minutes > 0:
            time_parts.append(f"{minutes}m")

        # Combine the time parts into a single string
        formatted_days_hours_minutes = " ".join(time_parts)

        # Total hours and minutes in HH:MM format
        total_hours = int(time_diff.total_seconds() // 3600)
        total_minutes = int((time_diff.total_seconds() % 3600) // 60)
        formatted_total_hours = f"{total_hours:02}:{total_minutes:02}"

        # Calculate decimal format (hours + minutes/60)
        decimal_hours = total_hours + (total_minutes / 60)

        # Store the results in the 'Label_Created' row
        label_created_index = label_created_row.name
        df_sorted.at[label_created_index, 'Label to Arrive Days/Hour/Minute'] = formatted_days_hours_minutes
        df_sorted.at[label_created_index, 'Label to Arrive Hour/Minute'] = formatted_total_hours
        df_sorted.at[label_created_index, 'Label to Arrive Hour/Minute decimal'] = round(decimal_hours, 2)  # Decimal format rounded to 2 decimals

# Save the modified DataFrame to a new CSV file
output_file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\hours_days_with_calculations_v2.csv'
df_sorted.to_csv(output_file_path, index=False)

print(f"Processed file saved as {output_file_path}")











