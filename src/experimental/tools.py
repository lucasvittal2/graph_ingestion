import logging
import json


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