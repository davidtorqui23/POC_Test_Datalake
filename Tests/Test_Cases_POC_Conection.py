import logging
import os

from azure.storage.blob import BlobServiceClient
from Users.User_Datalake import AZ_DL_KEY, AZ_DL_STORAGE_URL

global name_container
name_container = 'test-automation'

def test_conection():

    try:

        test_result = False
        
        # Inicialite the log
        logger = logging.getLogger(__name__)

        # Create the object for conection
        blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)

        # Validate the conection
        blob_service_client.get_account_information()

        # Value for the assertion
        test_result = True
        logger.warning(f"The conection is successful. To Container '{name_container}'.")

    except Exception as e:
        
        # Add to log history
        logger.error(f"The conection Fail. {e}")

    finally:

        assert test_result 

def test_download_json():

    try:

        test_result = False

        logger = logging.getLogger(__name__)

        # Create the object for conection
        blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)

        # Get the container client
        container_client = blob_service_client.get_container_client(name_container)
        
        # Name of the JSON file in the Azure Blob container
        blob_name = "Genoa/Data.json"

        # Get the current working directory
        current_location = os.getcwd()

        # Custom name and directory for the downloaded JSON file
        file_name = "data_validation.json"
        directory = f'{current_location}\Data'

        # Get the container and blob clients
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file from Azure Blob storage
        with open(os.path.join(directory, file_name), "wb") as file:
            data = blob_client.download_blob()
            file.write(data.readall())
        
        # Value for the assertion
        test_result = True
        logger.warning(f"JSON file saved as {file_name} in {directory}")

    except Exception as e:
        
        # Add to log history
        logger.error(f"The download Json Fail. {e}")
    
    finally:

        assert test_result 
