import os
import logging
from typing import Dict, List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from tools import read_json, setup_logs


def get_urls_metadata_from_file(metadata: Dict[str, dict]) -> dict:
    index = 1
    extracted_metadata = []
    total = len(metadata.items())
    logging.info("Starting extraction of url metadata process...")
    for pmc_id, record in metadata.items():
        available_fields = record["OA"].keys()
        if "records" not in available_fields:
            logging.warning(record["OA"]["error"]["#text"] + " Skipped this article.")
            continue

        url_metadata = record["OA"]["records"]["record"]["link"]
        if type(url_metadata) is dict:
            metadata_item = {
                "PMCID": pmc_id,
                "fileFormat": url_metadata["@format"],
                "updateDate": url_metadata["@updated"],
                "fileUrl": url_metadata["@href"],
            }
            extracted_metadata.append(metadata_item)
        elif type(url_metadata) is list:
            metadata_items = [
                {
                    "PMCID": pmc_id,
                    "fileFormat": item["@format"],
                    "updateDate": item["@updated"],
                    "fileUrl": item["@href"],
                }
                for item in url_metadata
            ]
            extracted_metadata.extend(metadata_items)

        logging.info(f"Extract {index} of {total} url metadata itens")
        index += 1


    return extracted_metadata


def get_all_url_metadata(url_raw_path: str) -> List[dict]:
    url_metadata = []
    files = [f"{url_raw_path}/{file}" for file in os.listdir(url_raw_path)]
    logging.info(f"Starting extraction url metadata...")
    for file in files:

        metadata = read_json(file)
        extracted_metadata = get_urls_metadata_from_file(metadata)
        url_metadata.extend(extracted_metadata)
        logging.info(f"Extracted url metadata successfully from {file}.")
        print("\n\n")

    return url_metadata

def save_data_parquet_spark(data: List[dict], path_to_save: str) -> None:

    logging.info("Starting the Spark application process of saving ingested data as parquet...\n\n")
    spark = SparkSession.builder \
        .appName("Pubmed Metadata ingestion") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
        logging.info(f"Created Save folder '{path_to_save}'.")

    schema = StructType([
        StructField("PMCID", StringType(), True),
        StructField("fileFormat", StringType(), True),
        StructField("fileUrl", StringType(), True),
        StructField("updateDate", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("overwrite").parquet(f"{path_to_save}/pubmed_ingested_url_metadata.parquet")
    print("\n\n")
    df.show(5)
    print('\n\n')
    logging.info("Finished spark application.")
    logging.info(f"Saved data saved as parquet at '{path_to_save}/pubmed_ingested_url_metadata.parquet' successfully !")

if __name__=="__main__":
    RAW_DATA_PATH = "data/raw/metadata/url"
    BRONZE_PATH = "data/bronze/23-05-2025/"

    setup_logs()
    url_metadata = get_all_url_metadata(RAW_DATA_PATH)
    save_data_parquet_spark(url_metadata, BRONZE_PATH)