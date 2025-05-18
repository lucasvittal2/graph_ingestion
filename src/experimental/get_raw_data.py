import os
import random
import requests
import threading
import logging
from typing import List
from tools import *
from concurrent.futures import ThreadPoolExecutor


def sample_ids()-> None:
    SAMPLE_SIZE = 50000
    PATH = "/home/acer/Downloads/PMC040XXXXX_json_ascii"
    pubmed_ids = [item.replace(".xml", "") for item in os.listdir(PATH)]
    sample = random.choices(pubmed_ids, k=SAMPLE_SIZE)
    save_text("|".join(sample), "/data/pubmed_ids_sample.txt")


def fetch_single_id(id, raw_data, lock):
    try:
        url = f"https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/{id}/unicode"
        response = requests.get(url)
        body_response = response.json()

        # Use a lock when writing to the shared dictionary
        with lock:
            raw_data[id] = body_response[0]
            logging.info(f"Got raw data for ID: {id}")

        return True
    except Exception as e:
        logging.error(f"Failed to get raw data for ID {id}: {e}")
        return False


def get_raw_data_threaded(ids: List[str], max_workers=10, continue_process=False):
    raw_data = {}
    lock = threading.Lock()  # Create a lock for thread-safe dictionary updates
    RAW_DATA_PATH = '/home/acer/projects/graph_ingestion/data/raw/pubmed_articles.json'
    ids_to_process = ids.copy()

    #implement incremental extractions
    if continue_process:
        all_ids_set = set(ids_to_process)
        content_processed = read_json(RAW_DATA_PATH)
        ids_processed = set(content_processed.keys())
        ids_to_process = list(all_ids_set.difference(ids_processed))

    total_ids = len(ids_to_process)
    successful_fetches = 0

    try:
        # Use a ThreadPoolExecutor to manage the thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each ID
            futures = [executor.submit(fetch_single_id, id, raw_data, lock) for id in ids_to_process]

            # Wait for all futures to complete
            for future in futures:
                if future.result():
                    successful_fetches += 1

        logging.info(
            f"Extraction of raw data completed. Successfully fetched {successful_fetches}/{total_ids} records.")
        save_json(raw_data, RAW_DATA_PATH)
        return raw_data

    except Exception as e:
        logging.error(f"Failed in thread execution: \n\n{e}\n\n")
        # Save whatever data was collected before the error
        if raw_data:
            save_json(raw_data, RAW_DATA_PATH)
        return raw_data



if __name__=="__main__":
    setup_logs()
    #sample  = sample_ids()
    pubmed_ids = read_text("/home/acer/projects/graph_ingestion/data/pubmed_ids_sample.txt").split("|")
    get_raw_data_threaded(pubmed_ids, max_workers=20)

