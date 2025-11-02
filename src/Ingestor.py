import os
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm


class Ingestor:
    """Data Ingestor class for handling API data ingestion with DRY principles."""

    def __init__(self, endpoint=None,
                 base_url=None):
        """
        Initialize API credentials and endpoint.
        """
        load_dotenv()

        # Load API credentials
        self.api_key_id = os.getenv('API_KEY_ID')
        self.api_secret = os.getenv('API_SECRET')

        # Build API endpoint URL

        self.url = f"{base_url}/{endpoint}"
        self.headers = {
            "X-Api-Key-Id": self.api_key_id,
            "X-Api-Secret": self.api_secret
        }
        if endpoint is None or base_url is None:
            raise ValueError("Endpoint and Base URL must be provided")
        else:
            print(f"Ingestor initailaized & API credentials loaded successfully")

    def fetchApi(self, columns, pastDays=60):
        """
        Fetches data from the API, shwoing progress with tqdm.
        Returns a dataframe 


        How it fetches:
        - fetches last 60 days of data by deafult
        - puts data in pages of 1000 records
        - uses $limit and $offset for pagination
        - applies row_filter if provided
        - fetches until max_records is reached or no more data

        into a all_data list, then converts to DataFrame.


        """

        # ROW FILTER : by default get data for last 60 days

        now = datetime.now()
        start_date = (now - timedelta(days=pastDays)
                      ).strftime("%Y-%m-%dT00:00:00")
        end_date = now.strftime("%Y-%m-%dT23:59:59")

        row_filter = f"(date>='{start_date}' AND date<='{end_date}')"
        print(f"ingesting last : {pastDays} days of data!")

        # how much to fetch &  Pagination parameters

        pagelimit = 1000
        max_records = 500
        timeout = 10
        delay = 4
        offset = 0

        all_data = []

        print(f"Fetching data from: {self.url}")
        print(f"Date range: {start_date} to {end_date}")
        print(f"Row filter: {row_filter}")
        print(f"Columns: {columns}")

        # Initialize progress bar
        with tqdm(total=max_records if max_records != float('inf') else None, desc="Fetching records") as pbar:
            while len(all_data) < max_records:
                params = {
                    "$limit": pagelimit,
                    "$offset": offset,
                    "$select": ','.join(columns) if columns else '*',
                    "$where": row_filter
                }

                try:
                    response = requests.get(
                        self.url, headers=self.headers, params=params, timeout=timeout)

                    if response.status_code == 200:
                        data = response.json()
                        if not data:
                            print("No data returned. Exiting loop.")
                            break

                        # Handle the case where max_records is float('inf')
                        if max_records == float('inf'):
                            records_to_add = data
                        else:
                            records_to_add = data[:max_records - len(all_data)]

                        all_data.extend(records_to_add)
                        pbar.update(len(records_to_add))
                        offset += pagelimit
                    else:
                        print(
                            f"Error: {response.status_code} - {response.text}")
                        raise Exception(
                            f"Failed to retrieve data: {response.status_code}")

                    time.sleep(delay)

                except requests.exceptions.Timeout:
                    print(f"Request timed out. Retrying...")
                    time.sleep(delay * 2)
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred: {e}")
                    break

        df = pd.DataFrame(all_data)
        print(f"Total records fetched: {len(df)}")

        # Display data insights
        print(f"\nData insights:")
        print(f"Shape: {df.shape}")
        print(f"Columns: {len(df.columns)}")
        print(f"Rows: {len(df)}")
        print(f"\nFirst few records:")
        print(df.head())

        print("data ingestion completed successfully and returning dataframe")

        return df

    def saveCSV(self, df, path='RawData/DataSet1', filePrefix='crimes_data'):
        """
        - saves file to specified directory e.g 'RawData/DataSet1'
        - filename includes timestamp to avoid overwriting
        - creates time stamp using current date and time

        """

        # Create directory if it doesn't exist
        os.makedirs(path, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filePrefix}_{timestamp}.csv"
        file_path = os.path.join(path, filename)

        # Save to CSV
        df.to_csv(file_path, index=False)

        # Verify file was created
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f"\n[Success] - Data saved to CSV: {file_path}")
            print(f"[Success] - File size: {file_size} bytes")

        else:
            print(f"[Error] - Failed to save CSV file: {file_path}")

        return file_path
