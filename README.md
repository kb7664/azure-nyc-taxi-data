# azure-nyc-taxi-data
This project ingests the NYC Taxi and Limousine yellow dataset using `azureml-opendatasets`, processes the data to calculate mean and median costs, prices, and passenger counts aggregated by payment type, year, and month. The results are output to a Parquet file.

## Prerequisites

* Python 3.11 

## Setup Instructions

1. Clone the repo:

    ```
    git clone git@github.com:kb7664/azure-nyc-taxi-data.git
    ```

2. Create  environment and install dependencies:

### For Windows

    Run the following commands

    ```bash
    pip install virtualenvwrapper-win
    mkvirtualenv --python=C:\path\to\Python311\python.exe azure_nyc_taxi_data
    workon azure_nyc_taxi_data
    pip install -r requirements.txt
    ```

### For macOS

    Run the following commands

    ```bash
    pyenv install 3.11.9 -s
    pyenv virtualenv 3.11.9 azure_nyc_taxi_data
    pip install -r requirements.txt
    ```

## Project Overview

This project ingests the NYC Taxi and Limousine yellow dataset using `azureml-opendatasets`, processes the data to calculate mean and median costs, prices, and passenger counts aggregated by payment type, year, and month. The results are output to both a Parquet file and a CSV.

### Input Data

The [input dataset](https://learn.microsoft.com/en-us/azure/open-datasets/dataset-taxi-yellow?tabs=azureml-opendatasets) includes the NYC Taxi and Limousine Commission (TLC) yellow taxi trip records, which capture various details of taxi trips such as:

- **pickup and drop-off dates/times**: When the trip started and ended.
- **pickup and drop-off locations**: Where the trip started and ended.
- **trip distances**: The distance covered during the trip.
- **itemized fares**: The fare breakdown including fare amount, extras, taxes, tips, tolls, and total amount.
- **rate types**: The type of rate applied to the trip.
- **payment types**: How the passenger paid for the trip (e.g., credit card, cash).
- **passenger counts**: The number of passengers in the vehicle.

The dataset spans multiple years and is stored in Parquet format, making it efficient to process large volumes of data.

### Process

The processing pipeline involves the following steps:

1. **Data Ingestion**: The dataset is ingested in batches to handle large volumes of data efficiently.
2. **Data Cleaning**:
    - Normalize `paymentType` values to a consistent format.
    - Filter out rows with invalid or missing values in critical columns such as `totalAmount` and `passengerCount`.
3. **Data Transformation**:
    - Convert datetime columns to appropriate datetime types.
    - Extract year and month from the pickup datetime for aggregation purposes.
    - Aggregate data to calculate mean and median costs, prices, and passenger counts grouped by `paymentType`, `year`, and `month`.
4. **Data Export**: The cleaned and aggregated data is exported to both Parquet and CSV formats for further analysis and easy accessibility.

### Output Data

The output data provides a summary of key metrics, including:

- **Mean and median total amounts**: The average and median fare amounts charged to passengers.
- **Mean and median passenger counts**: The average and median number of passengers per trip.

The data is aggregated by:

- **Payment Type**: How the fare was paid (e.g., credit card, cash).
- **Year**: The year when the trip occurred.
- **Month**: The month when the trip occurred.

## Running the script

A simple bash command serves as the entry point for the script. 

    ```bash
    python src/main.py
    ```

    The script will output a Parquet file to the `output/` directory.

### Running the unit tests

    ```bash
    set PYTHONPATH=src
    python -m unittest discover -s tests
    ```

## Handling Large Datasets

The current implementation reads data in chunks to manage memory usage efficiently. For larger datasets in a real-world scenario, consider using tools like Dask or Apache Spark for distributed processing.

## Future Improvements

- Implement batch processing using Apache Airflow for better scalability.
- Enhance data validation steps.
- Enhance error handling.
- Expand unit tests to cover more edge cases.

## Repository Link

You can find the complete project on GitHub: [azure-nyc-taxi-data](https://github.com/kb7664/azure-nyc-taxi-data)
