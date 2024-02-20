import pandas as pd
import numpy as np
import json
import time

from azure.storage.blob import BlobServiceClient
from Users.User_Datalake import AZ_DL_KEY, AZ_DL_STORAGE_URL

def generate_parquet():

    # Start Perform in time
    start_time = time.time()

    # Create the object for conection
    blob_service_client = BlobServiceClient(account_url=AZ_DL_STORAGE_URL, credential=AZ_DL_KEY)

    # Get the container client
    container_client = blob_service_client.get_container_client('test-automation/Genoa')

    # Open and read the JSON file
    with open('Data/data_validation.json', 'r') as file:
        data2 = json.load(file)

    numero_archivos = 2000

    for i in range(numero_archivos):

        if i in (0,1,2,3,4,5,6,7):
            continue

        # Generate random data
        num_rows = 1000
        num_columns = 8
        column_names = [f"col_{i}" for i in range(num_columns)]

        # Create a DataFrame with random data
        data = np.random.rand(num_rows, num_columns)
        df = pd.DataFrame(data, columns=column_names)

        # Save the DataFrame as a Parquet file
        name_file = f"parquet_{i+1}.parquet"

        # Get the container and blob clients
        blob_client = container_client.get_blob_client(name_file)

        blob_client.upload_blob(df.to_parquet(engine='pyarrow'))
        print(f"uploaded to Azure Blob container test-automation/Genoa as {name_file}")

        blob_client.get_blob_properties()

        name_blob = blob_client.get_blob_properties().name
        name_json = name_blob.replace('Genoa/', '')
        size_blob = blob_client.get_blob_properties().size
        new = {"name": name_json, "size":size_blob}
        data2.append(new)

        # Save the updated JSON data to a file
        with open("Data/data_validation.json", "w") as file:
            json.dump(data2, file, indent=2)

    # Finish Perform in time
    elapsed_time = time.time() - start_time
    print(f"Time taken to case: {elapsed_time:.2f} seconds")

generate_parquet()