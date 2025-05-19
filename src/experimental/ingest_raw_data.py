from pyspark.sql import SparkSession
from tools import *
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType

if __name__ == "__main__":
    # Path to your JSON file
    RAW_DATA_PATH = "/home/acer/projects/graph_ingestion/data/raw/19-05-2025/pubmed_articles_metadata_partition1.json"
    raw_data = read_json(RAW_DATA_PATH)

    # Create SparkSession with increased memory allocation
    spark = SparkSession.builder \
        .appName("Pubmed Raw Data Ingestion") \
        .master("local[*]") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "10") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")



    schema = StructType([
        StructField("pubmedid", MapType(StringType(), StringType()), True)
    ])

    # Try reading with explicit schema first
    print("Trying to read with explicit schema...")
    metadatas = spark.read.schema(schema).json(RAW_DATA_PATH)
    print(f"Successfully loaded data with {metadatas.count()} rows")
    print(metadatas.head(2))
    spark.stop()