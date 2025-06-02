import logging
import os
from logging import Logger
from typing import List

from langchain_postgres import PGVector
from langchain_core.embeddings import Embeddings
from langchain_core.documents import Document

connection = "postgresql+psycopg://langchain:langchain@localhost:6024/langchain"  # Uses psycopg3!
collection_name = "my_docs"


class PostgresDBVector:
    def __init__(self, collection_name: str, embeddings: Embeddings, logger: Logger) -> None:

        self._vector_store = PGVector(
            embeddings=embeddings,
            collection_name=collection_name,
            connection=os.environ["POSTEGRES_CONNECTION_STR"],
            logger=logger,
            use_jsonb=True,
        )
        self.logger = logger

    def __augment_metadata(self, docs: List[Document], metadata_records: List[dict]) -> List[Document]:
        self.logger.info("Augmenting metadata...")
        total = len(docs)
        i = 1
        docs_cp = docs.copy()

        for doc, metadata in zip(docs_cp, metadata_records):
            doc.metadata = {**doc.metadata, **metadata}
            self.logger.info(f"Augmented {i} of {total} metadata records")
            i+=1

        self.logger.info("Metadata Augmentation finished successfully.")
        print('\n')
        return docs_cp

    def __delete_all_documents(self) -> None:
        try:

            self._vector_store.delete_collection()
            self.logger.info(f"Deleted all documents successfully from '{self._vector_store.collection_name}.'")
            self._vector_store.create_collection()
        except Exception as err:
            self.logger.error(f"Error when delete documents from '{self._vector_store.collection_name}': \n\n{err}\n\n'")
            raise err

    def ingest_documents(self, docs: List[Document], metadata_records: List[dict]| None = None) -> None:
        total_docs = len(docs)

        docs_cp = docs.copy()
        ids = [doc.id for doc in docs_cp]

        if metadata_records is not None:
            total_metadata_records = len(metadata_records)

            if total_metadata_records != total_docs:
                raise ValueError("Metadata records should be in same amount than document.")

            docs_cp = self.__augment_metadata(docs, metadata_records)


        try:
            self.__delete_all_documents()
            self._vector_store.add_documents(docs_cp, ids=ids)
            self.logger.info(f"Added {total_docs} successfully.")
        except Exception as err:
            logging.error(f"Failed to add documents due to: \n\n{err}\n\n")
            raise err


    def query_contexts(self, query: str, filter: dict = None, top_k: int = 10) -> List[Document] | None:
        try:
            contexts = self._vector_store.similarity_search(query, filter=filter, k=top_k)
            self.logger.info(f"Retrieved contexts for query '{query}' with success.")
            return contexts
        except Exception as err:
            self.logger.error(f"Error when retrieve context for query '{query}: \n\n{err}\n\n'")
            raise err



if __name__ == "__main__":
    from langchain_community.document_loaders import PyPDFLoader
    from langchain_text_splitters import TokenTextSplitter
    from langchain_openai import OpenAIEmbeddings
    from constants import OPENAI_EMBEDDING_MODEL
    from app_logging import LoggerHandler
    from dotenv import load_dotenv
    import openai
    import os

    ARTICLE_METADATA = {
        "pmc_id": "PMC4001436",
        "pmid": "4001436",
        "title": "An easy method for preparing radioactive methyl esters of eicosanoids suitable as ligands in radioimmunoassays.",
        "abstract": "A rapid and convenient method is described for methylating prostanoids and other arachidonic acid metabolites. With 3H-methyl iodide of high specific activity, tracers for radioimmunoassay can be produced at a cost which is only a fraction of that of labeled compounds currently available. In radioimmunoassays for PGE2, TXB2 and 6-keto-PGF1 alpha, labeled methyl esters gave results which were comparable to those obtained with the use of tritiated free acids as radioligands.",
        "mesh_major_id": ["D004537"],
        "mesh_major_terms": ["Eicosanoic Acids"],
        "mesh_minor_ids": ["D007553", "D008745", "D011863", "D014316"],
        "mesh_minor_terms": ["Isotope Labeling", "Methylation", "Radioimmunoassay", "Tritium"],
        "date_revised": "2019-08-24",
        "date_completed": "1985-07-17",
        "date_created": "2014-04-28 12:54:26",
        "pdf_url": "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_pdf/fd/55/10.1177_2047981614523415.PMC4001436.pdf"
    }

    load_dotenv(".env")
    logger_handler = LoggerHandler(logger_name="TESTING-DOC-INGESTION", logging_type='console')
    openai.api_key = os.environ["OPENAI_API_KEY"]
    openai_embeddings = OpenAIEmbeddings(model=OPENAI_EMBEDDING_MODEL)
    vector_db = PostgresDBVector(embeddings = openai_embeddings, collection_name="test", logger=logger_handler.get_logger())
    pdf_text = PyPDFLoader("data/raw/data/pdf/10.1177_2047981614523415.PMC4001436.pdf").load()
    text_splitter = TokenTextSplitter(chunk_size=256, chunk_overlap=24)
    docs = text_splitter.split_documents(pdf_text)

    vector_db.ingest_documents(docs, [ARTICLE_METADATA]*len(docs))
    contexts = vector_db.query_contexts("test")
    #print(contexts)
