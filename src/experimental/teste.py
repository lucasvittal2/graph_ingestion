from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("teste") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

data = spark.read.parquet("data/silver/24-05-2025/pubmed_cleaned_data.parquet")
data.show(10)