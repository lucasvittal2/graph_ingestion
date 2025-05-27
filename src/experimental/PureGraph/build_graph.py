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
from langchain.schema import Document
from utils import map_to_base_node, map_to_base_relationship
from dotenv import load_dotenv
import openai
import os

class Neo4jChainGraphDB:

    def __init__(self, db_url: str):

        load_dotenv(".env")
        openai.api_key = os.environ["OPENAI_API_KEY"]
        NEO4J_USER = os.environ["NEO4J_USER"]
        NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

        self._graph  = Neo4jGraph(
        url=db_url,
        username=NEO4J_USER,
        password=NEO4J_PASSWORD,
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

        return create_structured_output_chain(KnowledgeGraph, self._openai_model, prompt_template, verbose=False)

    def __extract_and_store_graph(
        self,
        document: Document,
        nodes:Optional[List[str]] = None,
        rels:Optional[List[str]]=None) -> None:
        if not isinstance(document, Document):
            raise TypeError(f"Expected document to be an instance of Document, got {type(document)}")


        extract_chain = self.__get_extraction_chain(nodes, rels)
        data = extract_chain.invoke({"input": document.page_content})['function']


        graph_document = GraphDocument(
          nodes = [map_to_base_node(node) for node in data.nodes],
          relationships = [map_to_base_relationship(rel) for rel in data.rels],
          source = document
        )
        self._graph.add_graph_documents([graph_document],True)

    def build_graph(self, documents: List[Document]):
        for i, d in tqdm(enumerate(documents), total=len(documents)):
            #print(f"Processing chunk {i}: {d}")
            self.__extract_and_store_graph(d)
            print("Graph stored successfully.")

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
    from langchain_community.document_loaders import WebBaseLoader
    from langchain_text_splitters import TokenTextSplitter
    from constants import NEO4J_URL
    from tqdm import tqdm

    #setup
    load_dotenv(".env")
    graph_db = Neo4jChainGraphDB(db_url=NEO4J_URL)


    #build knowldge base
    raw_documents = WebBaseLoader("https://blog.langchain.dev/what-is-an-agent/").load()
    text_splitter = TokenTextSplitter(chunk_size=2048, chunk_overlap=24)
    documents = text_splitter.split_documents(raw_documents)
    graph_db.build_graph(documents)

    answer = graph_db.query_knowlodge_base("What is Ai Agent ?")
    print(answer)
