import os
from typing import Dict, List
from tools import read_json, setup_logs
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import logging


def get_values(path: str) -> List[Dict]:
    files = [f"{path}/{file}" for file in os.listdir(path)]
    data = []

    total = len(files)
    for i, file in enumerate(files):
        content = read_json(file)
        pubmeids = list(content.keys())

        for id in pubmeids:
            passages = content[id]["documents"][0]["passages"]
            title = content[id]["documents"][0]["passages"][0]["text"]

            for doc in passages[1:]:
                record = {
                    "pubmed_id": id,
                    "title": title,
                    "section_type": doc["infons"]["type"],
                    "text": doc["text"]
                }
                data.append(record)

        logging.info(f"Collected {i+1} of {total}")

    logging.info("finished data collection successfully !")
    return data


def save_data_parquet_spark(data: List[dict], path_to_save: str, spark: SparkSession) -> None:

    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
        logging.info(f"Created Save folder '{path_to_save}'.")

    schema = StructType([
        StructField("pubmed_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("section_type", StringType(), True),
        StructField("text", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("overwrite").parquet(f"{path_to_save}/pubmed_ingested_data.parquet")
    logging.info(f"Ingested data saved as parquet at '{path_to_save}' successfully !")
    pandas_df = df.toPandas()
    logging.info(f"A Sample of ingested data:\n\n")
    print(pandas_df.head(5))
    print("\n\n")



if __name__ == "__main__":
    RAW_DATA_PATH = "data/raw/20-05-2025/"
    BRONZE_PATH = "data/bronze/20-05-2025/"
    date = RAW_DATA_PATH.split('/')[-2]

    setup_logs()

    spark = SparkSession.builder \
        .appName("MySparkApplication") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    articles = get_values(RAW_DATA_PATH)
    save_data_parquet_spark(articles, BRONZE_PATH, spark)
    logging.info("Data Ingestion Completed Successfully !")
