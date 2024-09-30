import pandas as pd

# Load the CSV file from the path you provided
file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\ups_match_9_18_24.csv'
df_sorted = pd.read_csv(file_path)

# Convert 'Date' and 'Time' columns to proper datetime formats for calculation
df_sorted['Date'] = pd.to_datetime(df_sorted['Date'], format='%m/%d/%Y', errors='coerce')
df_sorted['Time'] = pd.to_timedelta(df_sorted['Time'], errors='coerce')

# Create new columns for the total delivery time calculation
df_sorted['Label to Delivery Days/Hour/Minute'] = None
df_sorted['Label to Delivery Hour/Minute'] = None
df_sorted['Label to Delivery Hour/Minute Decimal'] = None  # New column for decimal format

# Group by 'Tracking_Number' for efficient processing
grouped_df = df_sorted.groupby('Tracking_Number')

# Iterate over each group (Tracking_Number) and calculate the total time from 'Label Created' to 'Delivery'
for tracking_number, group in grouped_df:
    # Ensure the group has both "Label Created" (Step 1) and "Delivery" (Step 5) events
    if 1 in group['Seq'].values and 5 in group['Seq'].values:
        # Get the 'Label Created' (Step 1) event
        label_created_row = group[group['Seq'] == 1].iloc[0]
        label_created_datetime = label_created_row['Date'] + label_created_row['Time']

        # Get the 'Delivery' (Step 5) event
        delivery_row = group[group['Seq'] == 5].iloc[0]
        delivery_datetime = delivery_row['Date'] + delivery_row['Time']

        # Calculate the time difference
        time_diff = delivery_datetime - label_created_datetime

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

        # Correct the format for total time to only show hours and minutes within a 24-hour cycle
        total_hours = (days * 24) + hours
        formatted_total_hours = f"{total_hours:02}:{minutes:02}"

        # Calculate total hours in decimal format (hours + minutes as a fraction of 60)
        total_hours_decimal = total_hours + (minutes / 60)

        # Store the results in the 'Label Created' row (Step 1) without affecting other data
        label_created_index = label_created_row.name
        df_sorted.at[label_created_index, 'Label to Delivery Days/Hour/Minute'] = formatted_days_hours_minutes
        df_sorted.at[label_created_index, 'Label to Delivery Hour/Minute'] = formatted_total_hours
        df_sorted.at[label_created_index, 'Label to Delivery Hour/Minute Decimal'] = round(total_hours_decimal, 2)  # Decimal format

# Save the modified DataFrame to a new CSV file without deleting or overwriting previous data
output_file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\ups_match_total_delivery_time_corrected.csv'
df_sorted.to_csv(output_file_path, index=False)

print(f"Processed file saved as {output_file_path}")


