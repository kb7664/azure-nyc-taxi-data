from data_extraction import fetch_data
from data_transformation import process_data, export_data


def main():
    # This dataset contains data from 2009-01-01 to 2018-12-31 
    # A future enhancement could be to parameterize the dates and re-configure the extraction layer to hangle custom date ranges
    start_date = '2009-01-01'
    end_date = '2018-12-31'
    output_parquet_path = 'output/output.parquet'
    output_csv_path = 'output/output.csv'  # For easy accessibility
    try:
        # Fetch the data
        df = fetch_data(start_date, end_date, output_parquet_path, output_csv_path)
        print("Data processing and export completed successfully.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
