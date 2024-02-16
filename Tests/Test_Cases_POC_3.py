import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import io
import json
import os

from io import BytesIO
from fastparquet import ParquetFile
from azure.storage.blob import BlobServiceClient
from Users.User_Datalake import AZ_DL_KEY, AZ_DL_STORAGE_URL
from pyspark.sql import SparkSession

def read_json():

    try:
        logger = logging.getLogger(__name__)

        # Open and read the JSON file
        with open('Data/data_validation.json', 'r') as file:
            data = json.load(file)

        for datos in  data:
            print(datos['name'])

            if datos['name']=='parquet_3.parquet':
                print(datos['size'])

        # Get the number of records
        number_of_records = len(data)

        # Print the number of records
        print(f"Number of records: {number_of_records}")


    except Exception as e:
        
        # Add to log history
        logger.error(f"The read json Failed. {e}")

read_json()
