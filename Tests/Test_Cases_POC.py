import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import io

from io import BytesIO
from fastparquet import ParquetFile
from azure.storage.blob import BlobServiceClient
from Users.User_Datalake import AZ_DL_KEY, AZ_DL_STORAGE_URL

def test_validation_connection():

    try:
        validation_connection = True
        logger = logging.getLogger(__name__)

        # Create the object for conection
        blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)
        account_info = blob_service_client.get_account_information()

        # Handling of assertions
        validation_connection = (account_info['sku_name'] == 'Standard_LRS')

        # Add to log history
        logger.warning("Connection successful.")

    except Exception as e:

        # Add to log history
        logger.error(f"Error connecting to Standard_LRS: {e}")
    
    finally:

        # Validate the assert
        assert validation_connection 

def test_validation_container():

    try:
        validation_container = False
        name_container = 'test-automation'    

        logger = logging.getLogger(__name__)

        # Create the object for conection
        blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)

        containers = blob_service_client.list_containers()
    
        for container in containers:

            if container.name == name_container:

                validation_container = True

                 # Add to log history
                logger.warning(f"{name_container} - Container Exist.") 

                break  

    except Exception as e:
        
        # Add to log history
        logger.error(f"Error the container dont exist: {name_container} {e}")
    
    finally:

        # Validate the assert
        assert validation_container  

def test_validation_file():

    try:
        validation_file = False
        name_container = 'test-automation'
        name_file = 'test_automation.parquet'    

        logger = logging.getLogger(__name__)

        # Create the object for conection
        blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)

        # Get the container client
        container_client = blob_service_client.get_container_client(name_container)
        
        # Get the blob client
        blob_client = container_client.get_blob_client(name_file)

        if blob_client.exists() and name_file.endswith(".parquet"):

            validation_file = True

            # Add to log history
            logger.warning(f"{name_file} - File Exist.")  

    except Exception as e:
        
        # Add to log history
        logger.error(f"Error the file dont exist: {name_file} {e}")

    finally:

        # Validate the assert
        assert validation_file  

def test_validation_schema():

    try:

        validation_file = True
        name_container = 'test-automation'
        name_file = 'yellow_tripdata_2023-10.parquet'    

        logger = logging.getLogger(__name__)

        # Measure the time taken to perform the query on the DataFrame
        start_time = time.time()

        # Create the object for conection
        blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)

        # Get the container client
        container_client = blob_service_client.get_container_client(name_container)
        
        # Get the blob client
        blob_client = container_client.get_blob_client(name_file)

        # Download the parquet file as a stream
        stream = io.BytesIO()
        blob_client.download_blob().download_to_stream(stream)
        stream.seek(0)

        # Read the parquet file using pyarrow
        parquet_file = pq.ParquetFile(stream)

        # Define the expected schema using pandas
        expected_schema = pd.DataFrame({
            "names": ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
                      "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type","fare_amount",
                      "extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","congestion_surcharge","Airport_fee"],
            "types": ["int32", "timestamp[us]", "timestamp[us]", "int64","double", "int64","large_string","int32","int32","int64","double",
                      "double","double","double","double","double","double","double","double",]
        })

        # Compare the parquet schema with the expected schema
        parquet_schema = parquet_file.schema.to_arrow_schema()
        schema_tuples = [(field.name, field.type) for field in parquet_schema]

        for expected_name, expected_type in zip(expected_schema["names"], expected_schema["types"]):
            found = False
            for name, dtype in schema_tuples:
                if expected_name == name and expected_type == str(dtype):
                    found = True
                    break
            if not found:
                validation_file = False
                logger.error(f"Column {expected_name} with type {expected_type} is NOT present in the parquet file.")
                break
        
        elapsed_time = time.time() - start_time

        if validation_file == True:        
            # Add to log history
            logger.warning(f"{name_file} - The File has all the columns as defined.")
            logger.warning(f"Time taken to case: {elapsed_time:.2f} seconds") 

    except Exception as e:
        
        # Add to log history
        logger.error(f"Error The schema is not the same as the one defined: {name_file} {e}")

    finally:

        # Validate the assert
        assert validation_file  

def test_validation_query():

    try:
        validation_query = True
        name_container = 'test-automation'
        name_file = 'yellow_tripdata_2023-10.parquet'
        query_column = 'tpep_dropoff_datetime'
        query_value = '10/1/2023 1:27 AM'     

        logger = logging.getLogger(__name__)

        # Measure the time taken to perform the query on the DataFrame
        start_time = time.time()

        # Create the object for conection
        blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)

        # Get the container client
        container_client = blob_service_client.get_container_client(name_container)
        
        # Get the blob client
        blob_client = container_client.get_blob_client(name_file)

        # Download the Parquet file content
        blob_data = blob_client.download_blob()

        # Read the Parquet file into a DataFrame
        with BytesIO(blob_data.readall()) as file_stream:
            parquet_file = ParquetFile(file_stream)
            df = parquet_file.to_pandas()
            
        # Measure the time taken to perform the query on the DataFrame
        query_result = df[df[query_column] == query_value]
        elapsed_time = time.time() - start_time

        if query_result.empty:
            validation_query = False
        else:
            logger.warning(f"Time taken to query {name_file} (column: {query_column}, value: {query_value}): {elapsed_time:.2f} seconds")

    except Exception as e:
        
        # Add to log history
        logger.error(f"Error in the Query: {e}")

    finally:

        # Validate the assert
        assert validation_query