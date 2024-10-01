import pandas as pd

# --------------------------
# Updated calculations_A.py
# --------------------------
def calculations_A(df_sorted):
    grouped_df = df_sorted.groupby('Tracking_Number')

    for tracking_number, group in grouped_df:
        if 'Label_Created' in group['Class'].values and 'Arrived_at_Facility' in group['Class'].values:
            label_created_row = group[group['Class'] == 'Label_Created'].iloc[0]
            label_created_datetime = label_created_row['Date'] + label_created_row['Time']

            arrived_at_facility_row = group[group['Class'] == 'Arrived_at_Facility'].iloc[0]
            arrived_at_facility_datetime = arrived_at_facility_row['Date'] + arrived_at_facility_row['Time']

            time_diff = arrived_at_facility_datetime - label_created_datetime

            days = time_diff.days
            hours, remainder = divmod(time_diff.seconds, 3600)
            minutes, _ = divmod(remainder, 60)

            # Update provided columns
            df_sorted.loc[group.index, 'Label to Arrive Days/Hour/Minute'] = f'{days}D {hours:02}hrs {minutes:02}m'
            df_sorted.loc[group.index, 'Label to Arrive Hour/Minute'] = f'{days * 24 + hours}:{minutes:02}'
            df_sorted.loc[group.index, 'Label to Arrive Hour/Minute decimal'] = round(days * 24 + hours + minutes / 60, 2)

    return df_sorted

# --------------------------
# Updated calculations_B.py
# --------------------------
def calculations_B(df_sorted):
    grouped_df = df_sorted.groupby('Tracking_Number')

    for tracking_number, group in grouped_df:
        if 'Arrived_at_Facility' in group['Class'].values and 'Departed_Facility' in group['Class'].values:
            arrived_at_facility_row = group[group['Class'] == 'Arrived_at_Facility'].iloc[0]
            arrived_at_facility_datetime = arrived_at_facility_row['Date'] + arrived_at_facility_row['Time']

            departed_facility_row = group[group['Class'] == 'Departed_Facility'].iloc[0]
            departed_facility_datetime = departed_facility_row['Date'] + departed_facility_row['Time']

            time_diff = departed_facility_datetime - arrived_at_facility_datetime

            days = time_diff.days
            hours, remainder = divmod(time_diff.seconds, 3600)
            minutes, _ = divmod(remainder, 60)

            df_sorted.loc[group.index, 'Arrive to Depart Days/Hour/Minute'] = f'{days}D {hours:02}hrs {minutes:02}m'
            df_sorted.loc[group.index, 'Arrive to Depart Hour/Minute'] = f'{days * 24 + hours}:{minutes:02}'
            df_sorted.loc[group.index, 'Arrive to Depart Hour/Minute Decimal'] = round(days * 24 + hours + minutes / 60, 2)

    return df_sorted

# --------------------------
# Updated calculations_C.py
# --------------------------
def calculations_C(df_sorted):
    grouped_df = df_sorted.groupby('Tracking_Number')

    for tracking_number, group in grouped_df:
        if 'Arrived_at_Facility' in group['Class'].values and 'Delivered' in group['Class'].values:
            arrived_at_facility_row = group[group['Class'] == 'Arrived_at_Facility'].iloc[0]
            arrived_at_facility_datetime = arrived_at_facility_row['Date'] + arrived_at_facility_row['Time']

            delivered_row = group[group['Class'] == 'Delivered'].iloc[0]
            delivered_datetime = delivered_row['Date'] + delivered_row['Time']

            time_diff = delivered_datetime - arrived_at_facility_datetime

            days = time_diff.days
            hours, remainder = divmod(time_diff.seconds, 3600)
            minutes, _ = divmod(remainder, 60)

            df_sorted.loc[group.index, 'Arrive to Delivery Days/Hour/Minute'] = f'{days}D {hours:02}hrs {minutes:02}m'
            df_sorted.loc[group.index, 'Arrive to Delivery Hour/Minute'] = f'{days * 24 + hours}:{minutes:02}'
            df_sorted.loc[group.index, 'Arrive to Delivery Hour/Minute Decimal'] = round(days * 24 + hours + minutes / 60, 2)

    return df_sorted

# --------------------------
# Updated calculations_D.py
# --------------------------
def calculations_D(df_sorted):
    grouped_df = df_sorted.groupby('Tracking_Number')

    for tracking_number, group in grouped_df:
        if 'Label_Created' in group['Class'].values and 'Delivered' in group['Class'].values:
            label_created_row = group[group['Class'] == 'Label_Created'].iloc[0]
            label_created_datetime = label_created_row['Date'] + label_created_row['Time']

            delivered_row = group[group['Class'] == 'Delivered'].iloc[0]
            delivered_datetime = delivered_row['Date'] + delivered_row['Time']

            time_diff = delivered_datetime - label_created_datetime

            days = time_diff.days
            hours, remainder = divmod(time_diff.seconds, 3600)
            minutes, _ = divmod(remainder, 60)

            df_sorted.loc[group.index, 'Label to Delivery Days/Hour/Minute'] = f'{days}D {hours:02}hrs {minutes:02}m'
            df_sorted.loc[group.index, 'Label to Delivery Hour/Minute'] = f'{days * 24 + hours}:{minutes:02}'
            df_sorted.loc[group.index, 'Label to Delivery Hour/Minute Decimal'] = round(days * 24 + hours + minutes / 60, 2)

    return df_sorted

# --------------------------
# Updated calculations_total.py
# --------------------------
def calculations_total(df_sorted):
    grouped_df = df_sorted.groupby('Tracking_Number')

    for tracking_number, group in grouped_df:
        total_days = 0
        total_hours = 0
        total_minutes = 0
        total_decimal_hours = 0.0

        # Sum for Days/Hour/Minute format
        for col in ['Label to Arrive Days/Hour/Minute', 'Arrive to Depart Days/Hour/Minute', 'Arrive to Delivery Days/Hour/Minute', 'Label to Delivery Days/Hour/Minute']:
            if pd.notnull(group[col].iloc[0]):
                time_str = group[col].iloc[0]
                days, hours, minutes = map(int, time_str.replace('D', '').replace('hrs', '').replace('m', '').split())
                total_days += days
                total_hours += hours
                total_minutes += minutes

        total_hours += total_minutes // 60
        total_minutes = total_minutes % 60

        # Sum for Hour/Minute format
        total_hour_minutes = 0
        for col in ['Label to Arrive Hour/Minute', 'Arrive to Depart Hour/Minute', 'Arrive to Delivery Hour/Minute', 'Label to Delivery Hour/Minute']:
            if pd.notnull(group[col].iloc[0]):
                hours, minutes = map(int, group[col].iloc[0].split(':'))
                total_hour_minutes += hours * 60 + minutes

        # Sum for Decimal format
        total_decimal_hours += sum(group[col].iloc[0] for col in ['Label to Arrive Hour/Minute decimal', 'Arrive to Depart Hour/Minute Decimal', 'Arrive to Delivery Hour/Minute Decimal', 'Label to Delivery Hour/Minute Decimal'])

        index = group.index[0]
        df_sorted.at[index, 'Total Days/Hour/Minute'] = f'{total_days}D {total_hours:02}hrs {total_minutes:02}m'
        df_sorted.at[index, 'Total Hour/Minute'] = f'{int(total_hour_minutes // 60)}:{int(total_hour_minutes % 60):02}'
        df_sorted.at[index, 'Total Hour/Minute Decimal'] = round(total_decimal_hours, 2)

    return df_sorted

# Main function to run all calculations
def main():
    file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\hours_days.csv'
    df_sorted = pd.read_csv(file_path)

    df_sorted = calculations_A(df_sorted)
    df_sorted = calculations_B(df_sorted)
    df_sorted = calculations_C(df_sorted)
    df_sorted = calculations_D(df_sorted)
    df_sorted = calculations_total(df_sorted)

    # Saving the final results
    output_file_path = r'C:\Users\fstim\OneDrive\Investa Garden\Trade Block\tracking info\ups_match_total.csv'
    df_sorted.to_csv(output_file_path, index=False)
    print(f"Processed file saved as {output_file_path}")

# Run the main function
main()

