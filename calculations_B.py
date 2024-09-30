import pandas as pd

# Load the CSV file
file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\ups_match.csv'
df = pd.read_csv(file_path)

# Ensure the 'Class' column is stripped of leading/trailing spaces
df['Class'] = df['Class'].str.strip()

# Convert 'Date' to datetime format (assumed mm/dd/yyyy)
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')

# Remove "0 days" from 'Time' column
df['Time'] = pd.to_timedelta(df['Time'].str.replace('0 days ', '', regex=False))

# Sort by 'Tracking_Number', 'Date', and 'Time' to ensure chronological order
df = df.sort_values(by=['Tracking_Number', 'Date', 'Time'])

# Initialize the columns for the time differences in different formats
df['Arrive to Depart Days/Hour/Minute'] = None
df['Arrive to Depart Hour/Minute'] = None
df['Arrive to Depart Hour/Minute Decimal'] = None

# Group the data by 'Tracking_Number' to process each Tracking_Number individually
grouped_df = df.groupby('Tracking_Number')

# Iterate over each group (Tracking_Number)
for tracking_number, tracking_df in grouped_df:

    # Ensure the group has both "Arrived_at_Facility" and "Departed_from_Facility"
    if 'Arrived_at_Facility' in tracking_df['Class'].values and 'Departed_from_Facility' in tracking_df['Class'].values:
        # Get the first 'Arrived_at_Facility' event
        arrived_at_facility_row = tracking_df[tracking_df['Class'] == 'Arrived_at_Facility'].iloc[0]
        arrived_at_facility_datetime = arrived_at_facility_row['Date'] + arrived_at_facility_row['Time']
        
        # Get the first 'Departed_from_Facility' event after 'Arrived_at_Facility'
        departed_from_facility_rows = tracking_df[tracking_df['Class'] == 'Departed_from_Facility']
        if not departed_from_facility_rows.empty:
            departed_from_facility_row = departed_from_facility_rows.iloc[0]
            departed_from_facility_datetime = departed_from_facility_row['Date'] + departed_from_facility_row['Time']

            # Calculate the time difference
            time_diff = departed_from_facility_datetime - arrived_at_facility_datetime

            # Days, hours, and minutes
            days = time_diff.days
            hours, remainder = divmod(time_diff.seconds, 3600)
            minutes, _ = divmod(remainder, 60)

            # 1. Format Days/Hour/Minute compactly
            time_parts = []
            if days > 0:
                time_parts.append(f"{days}D")
            if hours > 0:
                time_parts.append(f"{hours}hrs")
            if minutes > 0:
                time_parts.append(f"{minutes}m")

            # Combine the time parts into a single string
            formatted_days_hours_minutes = " ".join(time_parts)

            # 2. Total hours and minutes in HH:MM format
            total_hours = int(time_diff.total_seconds() // 3600)
            total_minutes = int((time_diff.total_seconds() % 3600) // 60)
            formatted_total_hours = f"{total_hours:02}:{total_minutes:02}"

            # 3. Calculate decimal format (hours + minutes as decimal)
            total_hours_decimal = total_hours + (total_minutes / 60)

            # **Store the results** in the 'Arrived_at_Facility' row for each format
            arrived_at_facility_index = arrived_at_facility_row.name
            df.at[arrived_at_facility_index, 'Arrive to Depart Days/Hour/Minute'] = formatted_days_hours_minutes
            df.at[arrived_at_facility_index, 'Arrive to Depart Hour/Minute'] = formatted_total_hours
            df.at[arrived_at_facility_index, 'Arrive to Depart Hour/Minute Decimal'] = round(total_hours_decimal, 2)

# Save the modified DataFrame to a new CSV file
output_file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\ups_match_with_calculations_v2.csv'
df.to_csv(output_file_path, index=False)

print(f"Processed file saved as {output_file_path}")








