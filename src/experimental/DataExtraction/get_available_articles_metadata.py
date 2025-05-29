import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tools import setup_logs
import os

def start_spark_application(app_name: str) -> SparkSession:
    logging.info(f"Starting Spark application {app_name}...\n\n")
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")\
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    print("\n\n")
    logging.info(f"Spark application {app_name} is now running !")

    return spark

def get_available_pdf_metadata(url_metadata_path: str, article_metadata_path: str, path_to_save:str, spark: SparkSession) -> None:
    logging.info("Getting available pdf metadata into a singles table...")
    url_metadata_df = spark.read.parquet(url_metadata_path)
    article_metadata_df = spark.read.parquet(article_metadata_path)

    available_articles_df = url_metadata_df.alias("url") \
        .join(article_metadata_df.alias("am"), col("url.PMCID") == col("am.PMCID"), "inner")\
        .select([
                "url.PMCID","pubmedId", "title","abstract","meshMajorIds",
                "meshMajorTerms","meshMinorIds","meshMinorTerms","revisedDate",
                "completedDate","updateDate","fileUrl"
        ])\
        .dropDuplicates(["PMCID"])

    available_articles_df.show(5)
    file_path = f"{path_to_save}/available_articles_metadata.parquet"
    available_articles_df.write.parquet(file_path, mode="overwrite")
    available_articles_df.write.jdbc(
        url="jdbc:postgresql://localhost:5432/vectorstore",
        table="articles_metadata",
        mode="overwrite",
        properties={
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver"
        }
    )
    logging.info(f"Saved available articles metadata at '{file_path}.'")

if __name__=="__main__":
    GOLD_PATH = "data/gold"
    SILVER_PATH ="data/silver"

    today = datetime.date.today().strftime("%d-%m-%Y")
    last_saved_date_silver = sorted(os.listdir(SILVER_PATH))[-1]
    cleaned_articles_metadata_path = f"{SILVER_PATH}/{last_saved_date_silver}/pubmed_cleaned_metadata.parquet"
    cleaned_url_metadata_path = f"{SILVER_PATH}/{last_saved_date_silver}/pubmed_cleaned_url_metadata.parquet"
    path_to_save = f"{GOLD_PATH}/{today}"

    setup_logs()
    spark = start_spark_application("Get available articles metadata")
    get_available_pdf_metadata(cleaned_articles_metadata_path, cleaned_url_metadata_path,path_to_save, spark)