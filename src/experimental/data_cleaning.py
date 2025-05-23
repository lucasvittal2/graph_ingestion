import os
import logging
from datetime import date
from tools import setup_logs
from pyspark.sql import SparkSession
from pyspark.sql.functions import  lit, col, size
from pyspark.sql.dataframe import  DataFrame as SparkDataFrame

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

def clean_data(path: str, spark: SparkSession, today: str) -> SparkDataFrame:
    logging.info("Starting cleaning process...")
    data = spark.read.parquet(path)

    cleaned_data = data.filter(data.abstract.isNotNull())
    logging.info(f"Cleaned rows with 'Abstract' column with null values, remained {cleaned_data.count()} rows.")
    cleaned_data = cleaned_data.filter(data.pubmedId.isNotNull())
    logging.info(f"Cleaned rows with 'pubmedId' column with null values, remained {cleaned_data.count()} rows.")
    cleaned_data = cleaned_data.filter(data.title.isNotNull())
    logging.info(f"Cleaned rows with 'title' column with null values, remained {cleaned_data.count()} rows.")
    cleaned_data = cleaned_data.filter(size(col("meshMajorIds")) != 0)
    logging.info(f"Cleaned rows with 'meshMajorIds' column with no mesh ids, remained {cleaned_data.count()} rows.")
    cleaned_data = cleaned_data.filter(size(col("meshMajorTerms")) != 0)
    logging.info(f"Cleaned rows with 'meshMajorTerms' column with no mesh terms, remained {cleaned_data.count()} rows.")

    cleaned_data = cleaned_data.filter(size(col("meshMinorIds")) != 0)
    logging.info(f"Cleaned rows with ' meshMinorIds' column with no mesh ids, remained {cleaned_data.count()} rows.")
    cleaned_data = cleaned_data.filter(size(col("meshMinorTerms")) != 0)
    logging.info(f"Cleaned rows with 'meshMinorTerms' column with no mesh terms, remained {cleaned_data.count()} rows.")
    cleaned_data = cleaned_data.withColumn("cleanedDate", lit(today))


    logging.info("Data was cleaned successfully !")
    logging.info(f"Total row after cleaning process: {cleaned_data.count()} rows")

    return cleaned_data
def save_data(path_to_save:str, spark_df: SparkDataFrame) -> None:

    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
        logging.info(f"Created Save folder '{path_to_save}'.")

    cleaned_data_path = f"{path_to_save}/pubmed_cleaned_data.parquet"
    logging.info("Saving cleaned data...")
    spark_df.write.parquet(cleaned_data_path)
    logging.info(f"Cleaned data saved at {cleaned_data_path}")

if __name__=="__main__":
    BRONZE_DATA_PATH =  "data/bronze/22-05-2025/pubmed_ingested_data.parquet"

    today = date.today().strftime("%d-%m-%Y")
    silver_data_path = f"data/silver/{today}/"

    #Cleaning process
    setup_logs()
    spark_app = start_spark_application("Cleaning Pubmed Data")
    cleaned_data = clean_data(BRONZE_DATA_PATH,  spark_app, today)
    save_data(silver_data_path, cleaned_data)
