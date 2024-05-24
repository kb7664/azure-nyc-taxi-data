from azureml.opendatasets import NycTlcYellow
from dateutil import parser
import pandas as pd


# Function to fetch the entire dataset for a given date range
# In a robust system, scalability and performance would be improved by reading/processing the data in batches and in parallel. 
# For example: Apache Beam + Google Cloud Dataflow
def fetch_data(start_date_str, end_date_str):
    # Parse the input date strings to datetime objects
    start_date = parser.parse(start_date_str)
    end_date = parser.parse(end_date_str)
    # Initialize an empty DataFrame to store the results
    all_data = pd.DataFrame()
    # Iterate through each month in the date range
    current_date = start_date
    while current_date < end_date:
        next_date = (current_date + pd.DateOffset(months=1)).replace(day=1)
        print(f'Fetching data from {current_date.date()} to {next_date.date()}')
        # This connection is not always available on the first try so it may be helpful to add retry logic
        nyc_tlc = NycTlcYellow(start_date=current_date, end_date=next_date)
        df = nyc_tlc.to_pandas_dataframe()
        # Append the fetched data to the all_data DataFrame
        all_data = pd.concat([all_data, df], ignore_index=True)
        # Move to the next month
        current_date = next_date
    return all_data
