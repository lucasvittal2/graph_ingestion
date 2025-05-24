import os
import json
import logging
from typing import List


def save_text(data: str, path: str) -> None:
    try:
        with open(path, 'w') as file:
            file.write(data)
            logging.info(f"Save file successfully at {path} !")
    except Exception as e:
        logging.error(e)


def read_text(path: str) -> str | None:
    try:
        with open(path, 'r') as file:
            content = file.read()
            logging.info(f"Read file {path}")
            return content
    except Exception as e:
        logging.error(f"Failed to read file path {path}: \n\n{e}\n\n")
        return None


def save_json(data: str, path: str) -> None:
    try:
        with open(path, 'w') as file:
            json.dump(data, file)
            logging.info(f"Saved file {path} successfully !")

    except Exception as e:
        logging.error(f"Failed to save file path {path}: \n\n{e}\n\n")
        raise e


def read_json(path: str) -> dict | None:
    try:
        with open(path, 'r') as file:
            data = json.load(file)
            logging.info(f"Read file {path}")
            return data
    except Exception as e:
        logging.error(f"Failed to read file path {path}: \n\n{e}\n\n")
        raise e


def setup_logs() -> None:
    format = (
        f"[INGESTION-PUBMED-DATA] - [%(asctime)s] - [%(levelname)s] - %(message)s"
    )
    logging.basicConfig(format=format, level=logging.INFO)

def get_data_partitions(data: List[str], partition_size: int) -> List[List[str]]:
    data_size = len(data)
    partitions =  [data[i:i + partition_size] for i in range(0, data_size, partition_size)]
    num_partitions = len(partitions)
    logging.info(f"Partitioned data into {num_partitions} partitions.")
    return partitions

def create_save_folder_if_not_exists(path_to_save: str) -> None:
    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
        logging.info(f"Created Save folder '{path_to_save}'.")

def get_checkpoint_if_exists(checkpoint_file_path: str, partitions: List[List[str]]) -> List[List[str]]:

    if os.path.isfile(checkpoint_file_path):
        partition_checkpoint = read_json(checkpoint_file_path)["partition_index"]
        checkpoint_partitions = partitions[partition_checkpoint:]
    else:
        checkpoint_partitions = partitions

    return checkpoint_partitions