import os
import random
import requests
import threading
import logging
from typing import List
from tools import *
from concurrent.futures import ThreadPoolExecutor
from datetime import date


def sample_ids(sample_size: int)-> None:

    PATH = "/home/acer/Downloads/PMC040XXXXX_json_ascii"
    pubmed_ids = [item.replace(".xml", "") for item in os.listdir(PATH)]
    sample = random.choices(pubmed_ids, k=sample_size)
    save_text("|".join(sample), "/home/acer/projects/graph_ingestion/data/pubmed_ids_sample.txt")

def get_data_partitions(data: List[str], partition_size: int) -> List[List[str]]:
    data_size = len(data)
    partitions =  [data[i:i + partition_size] for i in range(0, data_size, partition_size)]
    num_partitions = len(partitions)
    logging.info(f"Partitioned data into {num_partitions} partitions.")
    return partitions

def fetch_single_id(id, raw_data, lock):
    try:
        url = f"https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/{id}/unicode"
        response = requests.get(url)
        body_response = response.json()

        # Use a lock when writing to the shared dictionary
        with lock:
            raw_data[id] = body_response[0]
            #logging.info(f"Got raw data for ID: {id}")

        return True
    except Exception as e:
        logging.error(f"Failed to get raw data for ID {id}: {e}")
        return False


def get_raw_data_threaded(ids: List[str], max_workers:int, num_partitions: int, raw_folder:str):


    today = date.today()
    formatted_date = today.strftime("%d-%m-%Y")
    lock = threading.Lock()
    path_to_save = f"{raw_folder}/{formatted_date}"
    partitions = get_data_partitions(pubmed_ids, num_partitions)
    checkpoint_file_path = f"{path_to_save}/checkpoint.json"

    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)

    #implement incremental extractions
    if os.path.isfile(checkpoint_file_path):
        partition_checkpoint = read_json(checkpoint_file_path)["partition_index"]
        partitions = partitions[partition_checkpoint:]

    total_ids = len(ids)
    successful_fetches = 0
    logging.info(f"Total records to process: {total_ids}")
    logging.info(f"Partitions to process: {num_partitions}")

    for i, partition in enumerate(partitions):

        raw_data = {}
        raw_data_path = f"{path_to_save}/pubmed_articles_metadata_partition{i + 1}.json"



        try:
            # Use a ThreadPoolExecutor to manage the thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit tasks for each ID
                futures = [executor.submit(fetch_single_id, id, raw_data, lock) for id in partition]

                # Wait for all futures to complete
                for future in futures:
                    if future.result():
                        successful_fetches += 1

            logging.info(
                f"Extraction of raw data completed. Successfully fetched {successful_fetches}/{total_ids} records.")
            save_json(raw_data, raw_data_path)



        except Exception as e:
            logging.error(f"Failed in thread execution: \n\n{e}\n\n")
            # Save whatever data was collected before the error
            logging.error(f"Saving current state...")
            if raw_data:
                save_json({"partition_index": i}, f"{path_to_save}/checkpoint.json")
                logging.error(f"{path_to_save}/checkpoint.json")

            raise e



    return None


if __name__=="__main__":
    SAMPLE_SIZE = 5000
    PARTITIONS= 100
    RAW_FOLDER = "/home/acer/projects/graph_ingestion/data/raw"

    setup_logs()
    sample  = sample_ids(SAMPLE_SIZE)
    pubmed_ids = read_text("/home/acer/projects/graph_ingestion/data/pubmed_ids_sample.txt").split("|")

    get_raw_data_threaded(pubmed_ids, max_workers=20, num_partitions=PARTITIONS, raw_folder=RAW_FOLDER)

