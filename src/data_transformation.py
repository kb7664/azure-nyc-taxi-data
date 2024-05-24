import os
import pandas as pd


def process_data(df):
    # Initial summary
    print("Initial Data Summary:")
    print(df.describe())
    print("Unique payment types before cleaning:")
    print(df['paymentType'].unique())
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
        'voided trip': 'Voided Trip'
    }
    # Cast paymentType to lower case and map to standardized values
    df['paymentType'] = df['paymentType'].str.lower().map(payment_type_map)
    # Drop rows with invalid payment types
    df = df.dropna(subset=['paymentType'])
    # Log the number of rows after cleaning payment types
    print(f"Number of rows after cleaning payment types: {len(df)}")
    # Remove rows with negative or zero values in totalAmount and passengerCount
    df = df[(df['totalAmount'] > 0) & (df['passengerCount'] > 0)]
    # Log the number of rows after removing invalid amounts
    print(f"Number of rows after removing invalid amounts: {len(df)}")
    # Aggregated data summary
    print("Aggregated Data Summary:")
    print(df.describe())
    # Aggregate data by payment type, year, and month
    agg_df = df.groupby(['paymentType', 'year', 'month']).agg({
        'totalAmount': ['mean', 'median'],
        'passengerCount': ['mean', 'median']
    }).reset_index()
    # Flatten MultiIndex columns
    agg_df.columns = ['_'.join(col).strip() if col[1] else col[0] for col in agg_df.columns.values]
    return agg_df


def export_data(df, parquet_path, csv_path):
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    # Save the DataFrame as a CSV file as well
    df.to_csv(csv_path, index=False)
