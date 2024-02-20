import time
import logging
import json

from azure.storage.blob import BlobServiceClient
from Users.User_Datalake import AZ_DL_KEY, AZ_DL_STORAGE_URL

global name_container
name_container = 'test-automation'

global format
format = ".parquet"

global folder
folder = "Genoa"


def conection():

    try:

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

        new_list = []

        for file in list:           

            if (file.name.endswith(format) and file.name.__contains__(folder)):
                new_list.append(file)

        return new_list
    
    except Exception as e:
        
        # Add to log history
        logger.error(f"Error try to get files. {e}")

def test_validation_number_files():

    try:

        logger = logging.getLogger(__name__)

        # Start Perform in time
        start_time = time.time()

        list = all_files()

        validation_number_files = False

        # Open and read the JSON file
        with open('Data/data_validation.json', 'r') as file:
            data = json.load(file)

        # Get the number of records
        number_of_records = len(data)
        
        # Assert
        if len(list) == number_of_records:
            
            validation_number_files = True

            # Add to log history
            logger.warning(f"The number the files: '{format}' in the folder: '{folder}' is: {len(list)}.")  

    except Exception as e:
        
        # Add to log history
        logger.error(f"The number the files is {len(list)} but expected {len(list)}. {e}")

    finally:

        # Finish Perform in time
        elapsed_time = time.time() - start_time
        logger.warning(f"Time taken to case: {elapsed_time:.2f} seconds")

        # Validate the assert
        assert validation_number_files 

def test_validation_size_files():

    try:

        logger = logging.getLogger(__name__)

        # Start Perform in time
        start_time = time.time()

        container_client = conection()
        lista = all_files()

        validation_size_files = True

        # Open and read the JSON file
        with open('Data/data_validation.json', 'r') as file:
            data = json.load(file)

        for list in lista:
            
            name_json = list.name
            name_json = name_json.replace('Genoa/', '')

            for datos in  data:
                if datos['name']==name_json:
                    expect_size = datos['size']
                    break

            # Get the blob client
            blob_client = container_client.get_blob_client(list)

            if not expect_size == blob_client.get_blob_properties().size:

                validation_size_files = False

                # Print the size
                logger.error(f"The file '{blob_client.get_blob_properties().name}' have the size: '{blob_client.get_blob_properties().size}' bytes but expected '{expect_size}'")
                
    except Exception as e:
        
        # Add to log history
        logger.error(f"The sizes are not correct. {e}")

    finally:

        # Finish Perform in time
        elapsed_time = time.time() - start_time
        logger.warning(f"Time taken to case: {elapsed_time:.2f} seconds")

        # Validate the assert
        assert validation_size_files  
