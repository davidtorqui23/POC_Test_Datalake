import json
import time


def velocity_json():

    # Start Perform in time
    start_time = time.time()

    value = 'parquet_2.parquet'

    # Open and read the JSON file
    with open('Data/data_validation.json', 'r') as file:
        data = json.load(file)

    for datos in  data:
        if datos['name']==value:
            expect_size = datos['size']
            print(expect_size)
            break

    # Finish Perform in time
    elapsed_time = time.time() - start_time
    print(f"Time taken to case: {elapsed_time:.2f} seconds")

velocity_json()