import pandas as pd

# Load the previously modified file containing Step 2
file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\ups_match.csv'
df_sorted = pd.read_csv(file_path)

# Convert 'Date' and 'Time' columns to proper datetime formats for calculation
df_sorted['Date'] = pd.to_datetime(df_sorted['Date'], format='%m/%d/%Y', errors='coerce')
df_sorted['Time'] = pd.to_timedelta(df_sorted['Time'], errors='coerce')

# Create new columns for the Step 3 calculations (Arrived at Facility to Delivery)
df_sorted['Arrive to Delivery Days/Hour/Minute'] = None
df_sorted['Arrive to Delivery Hour/Minute'] = None
df_sorted['Arrive to Delivery Hour/Minute Decimal'] = None

# Group by 'Tracking_Number' for efficient processing
grouped_df = df_sorted.groupby('Tracking_Number')

# Iterate over each group (Tracking_Number) and calculate the differences for 'Arrived at Facility' and 'Delivery'
for tracking_number, group in grouped_df:
    # Ensure the group has both "Arrived at Facility" (Step 4) and "Delivery" (Step 5) events
    if 4 in group['Seq'].values and 5 in group['Seq'].values:
        # Get the 'Arrived at Facility' (Step 4) event
        arrived_at_facility_row = group[group['Seq'] == 4].iloc[0]
        arrived_at_facility_datetime = arrived_at_facility_row['Date'] + arrived_at_facility_row['Time']

        # Get the 'Delivery' (Step 5) event
        delivery_row = group[group['Seq'] == 5].iloc[0]
        delivery_datetime = delivery_row['Date'] + delivery_row['Time']

        # Calculate the time difference
        time_diff = delivery_datetime - arrived_at_facility_datetime

        # Days, hours, and minutes
        days = time_diff.days
        hours, remainder = divmod(time_diff.seconds, 3600)
        minutes, _ = divmod(remainder, 60)

        # Format Days/Hour/Minute compactly (if 0 days, omit it)
        time_parts = []
        if days > 0:
            time_parts.append(f"{days}D")
        if hours > 0:
            time_parts.append(f"{hours}hrs")
        if minutes > 0:
            time_parts.append(f"{minutes}m")

        # Combine the time parts into a single string
        formatted_days_hours_minutes = " ".join(time_parts)

        # Total hours and minutes in HH:MM format (no seconds)
        total_hours = int(time_diff.total_seconds() // 3600)
        total_minutes = int((time_diff.total_seconds() % 3600) // 60)
        formatted_total_hours = f"{total_hours:02}:{total_minutes:02}"

        # Calculate decimal format (hours + minutes as decimal)
        total_hours_decimal = total_hours + (total_minutes / 60)

        # Store the results in the 'Arrived at Facility' row (Step 4)
        arrived_at_facility_index = arrived_at_facility_row.name
        df_sorted.at[arrived_at_facility_index, 'Arrive to Delivery Days/Hour/Minute'] = formatted_days_hours_minutes
        df_sorted.at[arrived_at_facility_index, 'Arrive to Delivery Hour/Minute'] = formatted_total_hours
        df_sorted.at[arrived_at_facility_index, 'Arrive to Delivery Hour/Minute Decimal'] = round(total_hours_decimal, 2)

# Save the modified DataFrame to a new CSV file without deleting any data from earlier steps
output_file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\ups_match_final_report.csv'
df_sorted.to_csv(output_file_path, index=False)

print(f"Processed file saved as {output_file_path}")





