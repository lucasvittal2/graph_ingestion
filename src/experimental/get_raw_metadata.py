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

def sample_ids(sample_size: int)-> None:

    PATH = "/home/acer/Downloads/PMC040XXXXX_json_ascii"
    pubmed_ids = [item.replace(".xml", "") for item in os.listdir(PATH)]
    sample = random.choices(pubmed_ids, k=sample_size)
    save_text("|".join(sample), "/home/acer/projects/graph_ingestion/data/pubmed_ids_sample.txt")


def fetch_single_id(id, raw_metadata, lock):
    try:
        url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        params = {
            "db": "pubmed",
            "id": id,
            "retmode": "xml"
        }

        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises HTTPError for bad responses

        if not response.content.strip().startswith(b"<"):
            logging.error(f"Response for ID {id} is not valid XML:\n{response.text}")
            return False

        body_response = xmltodict.parse(response.content)

        with lock:
            raw_metadata[id] = body_response
        return True

    except Exception as e:
        logging.error(f"Failed to get raw data for ID {id}: {e}")
        return False

def get_raw_metadata_threaded(ids: List[str], max_workers:int, num_partitions: int, raw_folder:str) -> None:

    total_ids = len(ids)
    today = date.today()
    lock = threading.Lock()
    successful_fetches = 0
    formatted_date = today.strftime("%d-%m-%Y")
    path_to_save = f"{raw_folder}/{formatted_date}"

    create_save_folder_if_not_exists(path_to_save)
    partitions = get_data_partitions(pubmed_ids, num_partitions)
    checkpoint_file_path = f"{path_to_save}/checkpoint.json"
    partitions = get_checkpoint_if_exists(checkpoint_file_path, partitions)
    logging.info(f"Total records to process: {total_ids}")
    logging.info(f"Partitions to process: {num_partitions}")


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
    SAMPLE_SIZE = 100000
    PARTITIONS= 2000
    RAW_FOLDER = "/home/acer/projects/graph_ingestion/data/raw/metadata"

    setup_logs()
    #sample  = sample_ids(SAMPLE_SIZE)
    pubmed_ids = read_text("/home/acer/projects/graph_ingestion/data/pubmed_ids_sample.txt").split("|")
    get_raw_metadata_threaded(pubmed_ids, max_workers=1, num_partitions=PARTITIONS, raw_folder=RAW_FOLDER)