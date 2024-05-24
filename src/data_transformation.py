import os
import pandas as pd


def process_data(df):
    # Convert pickup datetime to datetime type
    df['tpepPickupDateTime'] = pd.to_datetime(df['tpepPickupDateTime'])
    # Extract year and month from pickup datetime
    df['year'] = df['tpepPickupDateTime'].dt.year
    df['month'] = df['tpepPickupDateTime'].dt.month
    # Normalize paymentType values
    payment_type_map = {
        '1': 'Credit',
        '2': 'Cash',
        '3': 'No Charge',
        '4': 'Dispute',
        '5': 'Unknown',
        '6': 'Voided Trip',
        'crd': 'Credit',
        'csh': 'Cash',
        'no charge': 'No Charge',
        'dispute': 'Dispute',
        'unknown': 'Unknown',
        'voided trip': 'Voided Trip',
        'noc': 'No Charge',
        'unk': 'Unknown',
        'dis': 'Dispute',
        'voi': 'Voided Trip',
    }
    df['paymentType'] = df['paymentType'].str.lower().map(payment_type_map)
    # Drop rows with invalid payment types
    df = df.dropna(subset=['paymentType'])
    # Remove rows with negative or zero values in passengerCount
    df = df[(df['passengerCount'] > 0)]
    # Aggregate data by payment type, year, and month
    agg_df = df.groupby(['paymentType', 'year', 'month']).agg({
        'totalAmount': ['mean', 'median'],
        'passengerCount': ['mean', 'median']
    }).reset_index()
    # Flatten MultiIndex columns
    agg_df.columns = ['_'.join(col).strip() if col[1] else col[0] for col in agg_df.columns.values]
    return agg_df


def export_data(df, parquet_file_path, csv_file_path, append=False):
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)
    if append:
        # Save the DataFrame to a Parquet and a CSV file
        if os.path.exists(parquet_file_path):
            df.to_parquet(parquet_file_path, engine='fastparquet', append=True)
        else:
            df.to_parquet(parquet_file_path, engine='fastparquet')
        if os.path.exists(csv_file_path):
            df.to_csv(csv_file_path, index=False, mode='a', header=False)
        else:
            df.to_csv(csv_file_path, index=False)
    else:
        df.to_parquet(parquet_file_path, engine='fastparquet')
        df.to_csv(csv_file_path, index=False)
