import os
import random
import requests
import threading
import xmltodict
import logging
from typing import List
from tools import *
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import time

def fetch_single_id(id, raw_data, lock):
    try:
        url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={id}"

        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses

        if not response.content.strip().startswith(b"<"):
            logging.error(f"Response for ID {id} is not valid XML:\n{response.text}")
            return False

        body_response = xmltodict.parse(response.content)

        with lock:
            raw_data[id] = body_response
        return True

    except Exception as e:
        logging.error(f"Failed to get raw data for ID {id}: {e}")
        return False

def get_raw_metadata_threaded(ids: List[str], max_workers:int, partitions_size: int, raw_folder:str) -> None:

    total_ids = len(ids)
    today = date.today()
    lock = threading.Lock()
    successful_fetches = 0
    formatted_date = today.strftime("%d-%m-%Y")
    path_to_save = f"{raw_folder}/{formatted_date}"

    create_save_folder_if_not_exists(path_to_save)
    partitions = get_data_partitions(ids, partitions_size)
    checkpoint_file_path = f"{path_to_save}/checkpoint.json"
    partitions = get_checkpoint_if_exists(checkpoint_file_path, partitions)
    logging.info(f"Total records to process: {total_ids}")
    logging.info(f"Partitions to process: {total_ids/partitions_size} partitions")
    logging.info(f"Each partitition has size: {partitions_size} records")


    for i, partition in enumerate(partitions):

        raw_metadata = {}
        raw_data_path = f"{path_to_save}/pubmed_articles_metadata_partition{i + 1}.json"

        try:
            # Use a ThreadPoolExecutor to manage the thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit tasks for each ID
                futures = [executor.submit(fetch_single_id, id, raw_metadata, lock) for id in partition]

                # Wait for all futures to complete
                for future in futures:
                    if future.result():
                        successful_fetches += 1


            logging.info(
                f"Extraction of raw data completed. Successfully fetched {successful_fetches}/{total_ids} records.")
            save_json(raw_metadata, raw_data_path)

        except Exception as e:
            logging.error(f"Failed in thread execution: \n\n{e}\n\n")
            # Save whatever data was collected before the error
            logging.error(f"Saving current state...")
            if raw_metadata:
                save_json({"partition_index": i}, f"{path_to_save}/checkpoint.json")
                logging.error(f"{path_to_save}/checkpoint.json")

            raise e

if __name__=="__main__":

    PARTITIONS= 50
    WORKERS = 50
    RAW_FOLDER = "/home/acer/projects/graph_ingestion/data/raw/metadata/url"

    setup_logs()
    pmc_ids = read_text("/home/acer/projects/graph_ingestion/data/pubmed_ids_sample.txt").split("|")
    partitions_size = int(len(pmc_ids)/PARTITIONS)
    get_raw_metadata_threaded(pmc_ids, max_workers=WORKERS, partitions_size=partitions_size, raw_folder=RAW_FOLDER)