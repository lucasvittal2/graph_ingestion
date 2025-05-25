import os
import logging
from typing import List
from datetime import date
from pyspark.sql import SparkSession
from tools import read_json, setup_logs
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


def get_title(metadata: dict) -> str | None:

    if metadata["PubmedArticleSet"] is None:
        return None

    article_metadata = metadata["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]["Article"]
    title = article_metadata["ArticleTitle"]
    return title

def get_pubmed_id(metadata: dict) -> str | None:

    if metadata["PubmedArticleSet"] is None:
        return None

    article_metadata = metadata["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]
    pubmed_id = article_metadata["PMID"]["#text"] if "PMID" in article_metadata.keys() else None
    return pubmed_id

def get_abstract(metadata: dict) -> str | None:
    if metadata["PubmedArticleSet"] is None:
        return None

    article_metadata = metadata["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]["Article"]
    abstract = article_metadata["Abstract"]["AbstractText"] if "Abstract" in article_metadata.keys() else None
    return abstract


def get_mesh_data(metadata: dict) -> dict:

    empty_mesh_data = {
        "meshMajorIds": None,
        "meshMajorTerms": None,
        "meshMinorIds": None,
        "meshMinorTerms": None
    }

    if not metadata.get("PubmedArticleSet"):
        return empty_mesh_data

    try:
        med_citation = metadata["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]
    except KeyError:
        return empty_mesh_data

    if "MeshHeadingList" not in med_citation:
        return empty_mesh_data

    mesh_headings = med_citation["MeshHeadingList"]["MeshHeading"]


    mesh_major_ids, mesh_major_terms = [], []
    mesh_minor_ids, mesh_minor_terms = [], []
    headings_list = [mesh_headings] if isinstance(mesh_headings, dict) else mesh_headings


    for heading in headings_list:
        term_data = heading["DescriptorName"]
        is_major = term_data["@MajorTopicYN"] == "Y"

        if is_major:
            mesh_major_ids.append(term_data["@UI"])
            mesh_major_terms.append(term_data["#text"])
        else:
            mesh_minor_ids.append(term_data["@UI"])
            mesh_minor_terms.append(term_data["#text"])

    return {
        "meshMajorIds": mesh_major_ids,
        "meshMajorTerms": mesh_major_terms,
        "meshMinorIds": mesh_minor_ids,
        "meshMinorTerms": mesh_minor_terms
    }

def get_dates(metadata:dict)-> dict :
    if metadata["PubmedArticleSet"] is None:
        dates_data = { "revisedDate": None,"completed_date": None}
        return dates_data

    med_citation_metadata = metadata["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]
    revised_date_list = list(med_citation_metadata["DateRevised"].values())
    completed_date_list = list(med_citation_metadata["DateCompleted"].values())
    dates_data = {
        "revisedDate": '-'.join(revised_date_list),
        "completedDate": '-'.join(completed_date_list)
    }
    return dates_data

def ingest_data(path: str):
    json_files = [f"{path}/{file}" for file in os.listdir(path)]
    ingested_date = date.today().strftime("%d-%m-%Y")
    ingested_data = []

    for file in json_files:
        json_content = read_json(file)
        pmc_ids = list(json_content.keys())
        metadata_db = json_content.values()
        for pmc_id, metadata in zip(pmc_ids, metadata_db):
            title = get_title(metadata)
            pubmed_id = get_pubmed_id(metadata)
            abstract = get_abstract(metadata)
            mesh_data = get_mesh_data(metadata)
            dates_data = get_dates(metadata)
            ingested_data.append({
                "PMCID": pmc_id,
                "pubmedId": pubmed_id,
                "title": title,
                "abstract": abstract,
                **mesh_data,
                **dates_data,
                "ingestionDate": ingested_date
            })

    return ingested_data

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
        StructField("pubmedId", StringType(), True),
        StructField("title", StringType(), True),
        StructField("abstract", StringType(), True),
        StructField("meshMajorIds",  ArrayType(StringType()), True),
        StructField("meshMajorTerms",  ArrayType(StringType()), True),
        StructField("meshMinorIds", ArrayType(StringType()), True),
        StructField("meshMinorTerms",  ArrayType(StringType()), True),
        StructField("revisedDate", StringType(), True),
        StructField("completedDate", StringType(), True),
        StructField("ingestionDate", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("overwrite").parquet(f"{path_to_save}/pubmed_ingested_metadata.parquet")

    print('\n\n')
    logging.info("Finished spark application.")
    logging.info(f"Saved data saved as parquet at '{path_to_save}/pubmed_ingested_metadata.parquet' successfully !")



if __name__ == "__main__":
    #set parameters
    RAW_DATA_PATH = "data/raw/metadata/23-05-2025/"
    BRONZE_PATH = "data/bronze/23-05-2025/"

    #execution
    setup_logs()
    ingested_data = ingest_data(RAW_DATA_PATH)
    save_data_parquet_spark(ingested_data, BRONZE_PATH)
    logging.info("Data Ingestion Completed Successfully !")
