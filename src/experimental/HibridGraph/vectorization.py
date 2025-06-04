import pandas as pd
from utils import *
from logging import Logger
from vector_db import PostgresDBVector
from langchain_core.documents import  Document
from langchain_core.embeddings import Embeddings

def extract_mesh_terms(df: pd.DataFrame) -> pd.DataFrame:
    all_mesh_terms = []
    for mesh_list in df["meshMajorTerms"]:
        all_mesh_terms.extend(parse_mesh_terms(mesh_list))

    # Deduplicate terms
    unique_mesh_terms = list(set(all_mesh_terms))

    # Create a DataFrame of MeSH terms and their URIs
    mesh_df = pd.DataFrame({
        "meshTerm": unique_mesh_terms,
        "URI": [create_valid_uri("http://example.org/mesh", term) for term in unique_mesh_terms]
    })
    return mesh_df

class Vectorizer:

    def __init__(self, embeddings: Embeddings, logger: Logger) -> None:
        self._article_vector_db= PostgresDBVector(collection_name="articles",logger=logger, embeddings=embeddings)
        self._mesh_terms_vector_db = PostgresDBVector(collection_name="mesh_terms", logger=logger, embeddings=embeddings)
        self._logger = logger
        
    def vectorize_articles_dataframe(self, df: pd.DataFrame) -> None:
        docs = []
        i = 1
        total_row = len(df)
        self._logger.info("Extracting articles title, article_uri and meshMajorTerms fields into langchain document....")
        try:

            for index, row in df.iterrows():
                metadata= {
                    "title": row["title"],
                    "article_URI": create_article_uri(row["title"]),
                    "file_url": row["fileUrl"],
                    "mesh_major_terms": row["meshMajorTerms"]
                }
                doc = Document(page_content=row["abstract"], metadata=metadata)
                docs.append(doc)
                self._logger.info(f"Extracted {i} of {total_row}.")
                i+=1

            print("\n")
            self._logger.info("vectorizing title, article_uri and meshMajorTerms fields...")
            self._article_vector_db.ingest_documents(docs)
            self._logger.info("Vectorized title, article_uri and meshMajorTerms fields successfully.")
        except Exception as err:
            self._logger.error(f"Vectorization of title, article_uri and meshMajorTerms fields from metadata has failed \n\n{err}\n\n")
            raise err

    def vectorize_mesh_terms(self, df: pd.DataFrame) -> None:
        docs = []
        i = 1
        total_row = len(df)
        self._logger.info("Extracting mesh data into langchain document....")
        try:
            for index, row in df.iterrows():
                doc = Document(
                    page_content= row["meshTerm"],
                    metadata={"URI": row["URI"]}
                )
                docs.append(doc)
                self._logger.info(f"Extracted {i} of {total_row}.")
                i += 1

            self._logger.info("vectorizing mesh data...")
            self._mesh_terms_vector_db.ingest_documents(docs)
            self._logger.info("Vectorized mesh data successfully.")
                
        except Exception as err:
            self._logger.error(f"Vectorization of mesh data has failed \n\n{err}\n\n")
            raise err


if __name__ == "__main__":
    import os
    import openai
    from dotenv import load_dotenv
    from utils import start_spark_application
    from constants import OPENAI_EMBEDDING_MODEL
    from langchain_openai.embeddings import OpenAIEmbeddings
    from app_logging import LoggerHandler
    
    #setup paths
    GOLD_PATH = "data/gold"
    last_process_date = sorted(os.listdir(GOLD_PATH))[-1]
    metadata_path = f"{GOLD_PATH}/{last_process_date}/available_articles_metadata.parquet"
    
    #setup to do vectorization process
    load_dotenv(".env")
    openai.api_key = os.environ["OPENAI_API_KEY"]
    openai_embedding = OpenAIEmbeddings(model=OPENAI_EMBEDDING_MODEL)
    logger= LoggerHandler("TESTING-VECTORIZATION",logging_type="console").get_logger()

    vectorizer = Vectorizer(embeddings=openai_embedding, logger=logger)
    
    
    #do vectorization process
    spark = start_spark_application("Vectorizing metadata", logger)
    metadata_df = spark.read.parquet(metadata_path).toPandas()
    print(metadata_df.columns)
    mesh_df = extract_mesh_terms(metadata_df)
    vectorizer.vectorize_articles_dataframe(metadata_df)
    vectorizer.vectorize_mesh_terms(mesh_df)

    
    