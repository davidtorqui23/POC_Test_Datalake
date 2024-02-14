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
from pyspark.sql import SparkSession

def conection():

    try:
        name_container = 'test-automation'

        logger = logging.getLogger(__name__)

        # Create the object for conection
        blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)

        # Get the container client
        container_client = blob_service_client.get_container_client(name_container)
        
        return container_client

    except Exception as e:
        
        # Add to log history
        logger.error(f"The conection Fail. {e}")

def all_files():

    try:
 
        logger = logging.getLogger(__name__)

        container_client = conection()
        list = container_client.list_blobs()
        
        return list

    except Exception as e:
        
        # Add to log history
        logger.error(f"Error try to get files. {e}")

def test_validation_number_files():

    try:

        logger = logging.getLogger(__name__)

        lista = all_files()

        validation_number_files = False
        files_expected = 3
            
        format = ".parquet"
        folder = "Genoa"

        contador = 0


        for list in lista:

            if (list.name.endswith(format) and list.name.__contains__(folder)):
                contador += 1

        if contador == files_expected:

            validation_number_files = True

            # Add to log history
            logger.warning(f"The number the files: '{format}' in the folder: '{folder}' is: {contador}.")  

    except Exception as e:
        
        # Add to log history
        logger.error(f"The number the files is {contador} but expected {files_expected}. {e}")

    finally:

        # Validate the assert
        assert validation_number_files 

def test_validation_size_files():

    try:

        logger = logging.getLogger(__name__)

        container_client = conection()
        lista = all_files()

        validation_size_files = False
            
        format = ".parquet"
        folder = "Genoa"

        expect_dictionary = {'Genoa/parquet_1.parquet': 69289, 'Genoa/parquet_2.parquet': 106636, 'Genoa/parquet_3.parquet': 73233}
        size_in_parquets = {}

        for list in lista:

            if (list.name.endswith(format) and list.name.__contains__(folder)):

                # Get the blob client
                blob_client = container_client.get_blob_client(list)

                size_in_parquets[blob_client.get_blob_properties().name] = blob_client.get_blob_properties().size

                # Print the number of records
                logger.warning(f"The file '{blob_client.get_blob_properties().name}' have the size: '{blob_client.get_blob_properties().size}' bytes")
     
        if size_in_parquets == expect_dictionary:
                
                logger.warning(f"The sizes are correct")
                validation_size_files = True
                
    except Exception as e:
        
        # Add to log history
        logger.error(f"The sizes are not correct. {e}")

    finally:

        # Validate the assert
        assert validation_size_files  

 

def test_validation_number_records_files():

    try:

        logger = logging.getLogger(__name__)

        container_client = conection()
        lista = all_files()

        validation_number_records_files = False
            
        format = ".parquet"
        folder = "Genoa"

        expect_dictionary = {'Genoa/parquet_1.parquet': 368, 'Genoa/parquet_2.parquet': 853, 'Genoa/parquet_3.parquet': 403}
        records_in_parquets = {}

        for list in lista:

            if (list.name.endswith(format) and list.name.__contains__(folder)):

                # Get the blob client
                blob_client = container_client.get_blob_client(list)

                # Download the parquet file as a stream
                stream = io.BytesIO()
                blob_client.download_blob().download_to_stream(stream)
                stream.seek(0)

                # Read the .parquet file
                pf = ParquetFile(stream)

                # Convert the ParquetFile object to a pandas DataFrame
                df = pf.to_pandas()

                # Count the number of records
                record_count = len(df)

                records_in_parquets[list.name] = record_count

                # Print the number of records
                logger.warning(f"Number of records in the '{list.name}' file is : '{record_count}'")


        if records_in_parquets == expect_dictionary:
                
                logger.warning(f"The number of the records is correct")
                validation_number_records_files = True
                
    except Exception as e:
        
        # Add to log history
        logger.error(f"The number of the records is not correct. {e}")

    finally:

        # Validate the assert
        assert validation_number_records_files  
