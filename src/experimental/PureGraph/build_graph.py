from langchain.chains.openai_functions import (
    create_structured_output_chain,
)
from langchain_openai.chat_models import ChatOpenAI
from langchain_neo4j.graphs.neo4j_graph import Neo4jGraph, GraphDocument
from langchain_neo4j import GraphCypherQAChain
from langchain_core.prompts import ChatPromptTemplate
from graph_entities import KnowledgeGraph
from constants import *
from typing import Optional, List, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_log, after_log

from utils import *
from dotenv import load_dotenv
import openai
import logging

class Neo4jChainGraphDB:

    def __init__(self, db_url: str):

        load_dotenv(".env")
        openai.api_key = os.environ["OPENAI_API_KEY"]
        NEO4J_USER = os.environ["NEO4J_USER"]
        NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

        self._graph  = Neo4jGraph(
        url=db_url,
        username=NEO4J_USER,
        password=NEO4J_PASSWORD
        )

        self._openai_model = ChatOpenAI(model=OPENAI_MODEL_NAME, temperature=0)

    def __get_extraction_chain(
        self,
        allowed_nodes: Optional[List[str]] = None,
        allowed_rels: Optional[List[str]] = None
        ) -> Any:
        allowed_nodes = '- **Allowed Node Labels:**' + ", ".join(allowed_nodes) if allowed_nodes else ""
        allowed_rels = '- **Allowed Relationship Types**:' + ", ".join(allowed_rels) if allowed_rels else ""
        formatted_system_prompt = SYSTEM_PROMPT.format(
            allowed_nodes=allowed_nodes,
            allowed_relationships=allowed_rels
        )
        prompt_template = ChatPromptTemplate.from_messages(
            [(
              "system", formatted_system_prompt),
                ("human", "Use the given format to extract information from the following input: {input}"),
                ("human", "Tip: Make sure to answer in the correct format"),
            ])
        structured_output = create_structured_output_chain(KnowledgeGraph, self._openai_model, prompt_template, verbose=False)
        logging.info(f"Got the following strutured output to be stored on graph: \n\n{structured_output}\n\n")
        return structured_output

    def __extract_and_store_graph(
        self,
        document: Document,
        nodes:Optional[List[str]] = None,
        rels:Optional[List[str]]=None) -> None:
        if not isinstance(document, Document):
            raise TypeError(f"Expected document to be an instance of Document, got {type(document)}")


        extract_chain = self.__get_extraction_chain(nodes, rels)
        data = extract_chain.invoke({"input": str(document.page_content)})['function']
        logging.info(f"data extracted from doc: \n\n{data}\n\n")


        graph_document = GraphDocument(
          nodes = [map_to_base_node(node) for node in data.nodes],
          relationships = [map_to_base_relationship(rel) for rel in data.rels],
          source = document
        )
        self._graph.add_graph_documents([graph_document],True)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type( Exception),
        before=before_log(logging.getLogger(), log_level=logging.WARNING),
        reraise=True
    )
    def build_graph(self, documents: List[Document]):
        for i, d in tqdm(enumerate(documents), total=len(documents)):
            try:
                self.__extract_and_store_graph(d)
                logging.info("New knowledge stored successfully on knowledge graph.")
            except Exception as err:
                print(d)
                raise  err

    def query_knowlodge_base(self, query: str ) -> str:
        self._graph.refresh_schema()
        cypher_chain = GraphCypherQAChain.from_llm(
            graph=self._graph,
            cypher_llm=self._openai_model,
            qa_llm=self._openai_model,
            allow_dangerous_requests=True,
            validate_cypher=True,
            verbose=True,

        )
        answer = cypher_chain.invoke({"query": query})
        return answer

if __name__ == "__main__":

    from langchain_community.document_loaders import PyPDFLoader
    from langchain_text_splitters import TokenTextSplitter
    from constants import NEO4J_URL
    from tqdm import tqdm
    import os

    PMC_IDS = [
        "PMC4012372",
        "PMC4014813",
        "PMC4014814",
        "PMC4017398",
        "PMC4025424",
        "PMC4028520",
        "PMC4050920",
        "PMC4073339"
    ]
    PDF_PATH = "data/raw/data/pdf"
    pdf_paths = [f"{PDF_PATH}/{file}" for file in os.listdir(PDF_PATH) if file.split('.')[-2] in PMC_IDS]
    print(pdf_paths)
    graph_db = Neo4jChainGraphDB(db_url=NEO4J_URL)

    load_dotenv(".env")
    setup_logs()


    logging.info("starting process of building graph knowledge base...\n\n")
    resp = graph_db.query_knowlodge_base("what is the relationship between hormony therapy for women and menopause postergate ?")
    # try:
    #     for pdf_path in pdf_paths:
    #
    #         raw_documents = PyPDFLoader(pdf_path).load()
    #         sanitized_documents = sanitize_documents(raw_documents)
    #         text_splitter = TokenTextSplitter(chunk_size=256, chunk_overlap=24)
    #         documents = text_splitter.split_documents(sanitized_documents)
    #         graph_db.build_graph(documents)
    #         logging.info(f"add content from '{pdf_path} to knowledge graph successfully.")
    #
    #     logging.info(f"Graph knowledge base built on '{NEO4J_DB_NAME}' database")
    #
    # except Exception as err:
    #     logging.error(f"Graph knowledge base has failed: \n\n{err}\n\n")
