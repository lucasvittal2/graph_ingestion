import os
import logging
from datetime import date
from tools import setup_logs
from pyspark.sql import SparkSession
from pyspark.sql.functions import  lit, col, size
from pyspark.sql.dataframe import  DataFrame as SparkDataFrame
import shutil

def start_spark_application(app_name: str) -> SparkSession:
    logging.info(f"Starting Spark application {app_name}...\n\n")
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    print("\n\n")
    logging.info(f"Spark application {app_name} is now running !")

    return spark


def clean_data(path: str, spark: SparkSession, silver_last_dir: str) -> SparkDataFrame:
    logging.info("Starting cleaning process...")
    url_metadata = spark.read.parquet(path)
    general_metadata = spark.read.parquet(f"{silver_last_dir}/pubmed_cleaned_metadata.parquet")
    logging.info(f"Collected general metadata from '{silver_last_dir}'.")

    cleaned_data = url_metadata.alias("url") \
        .join(general_metadata.alias("gm"), col("url.PMCID") == col("gm.PMCID"), "left_semi") \
        .select(["url.PMCID","url.fileFormat", "url.updateDate", "url.fileUrl"])

    logging.info(f"Cleaned rows whose 'PMCID' column values is not identified in general cleaned metadata database, remained {cleaned_data.count()} rows.")
    cleaned_data = cleaned_data.filter(col("fileFormat") == lit("pdf"))
    logging.info(f"Cleaned rows with 'fileFormat' column with 'pdf' values, remained {cleaned_data.count()} rows.")


    cleaned_data.show(5)
    logging.info("Data was cleaned successfully !")
    logging.info(f"Total row after cleaning process: {cleaned_data.count()} rows")

    return cleaned_data

def save_data(path_to_save:str, spark_df: SparkDataFrame) -> None:

    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
        logging.info(f"Created Save folder '{path_to_save}'.")

    if os.path.exists(f"{path_to_save}/pubmed_cleaned_url_metadata.parquet"):
        shutil.rmtree(f"{path_to_save}/pubmed_cleaned_url_metadata.parquet")

    cleaned_data_path = f"{path_to_save}/pubmed_cleaned_url_metadata.parquet"
    logging.info("Saving cleaned data...")
    spark_df.write.parquet(cleaned_data_path)
    logging.info(f"Cleaned data saved at {cleaned_data_path}")

if __name__=="__main__":
    TODAY = date.today().strftime("%d-%m-%Y")
    last_dir_bronze = sorted(os.listdir("data/bronze"))[-1]
    last_dir_silver = sorted(os.listdir("data/silver"))[-1]

    bronze_data_path =  f"data/bronze/{last_dir_bronze}/pubmed_ingested_url_metadata.parquet"
    silver_data_path = f"data/silver/{last_dir_silver}"
    path_to_save = f"data/silver/{TODAY}"

    #Cleaning process
    setup_logs()
    spark_app = start_spark_application("Cleaning Pubmed Data")
    cleaned_data = clean_data(bronze_data_path,  spark_app, silver_data_path)
    save_data(path_to_save, cleaned_data)
