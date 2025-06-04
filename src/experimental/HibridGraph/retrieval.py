from utils import *
from SPARQLWrapper import SPARQLWrapper, JSON
from langchain_core.embeddings import Embeddings
from rdflib import Graph
from typing import List

class VectorKGRetriever:
    def __init__(self, embeddings: Embeddings, logger: Logger) -> None:

        self.logger = logger
        self.article_vector_db = PostgresDBVector(collection_name="articles", logger=logger, embeddings=embeddings)
        self.mesh_terms_vector_db = PostgresDBVector(collection_name="mesh_terms", logger=logger, embeddings=embeddings)


    def __get_alternative_terms(self, term: str) -> List[str]:
        term = sanitize_term(term)  # Sanitize input term
        sparql = SPARQLWrapper("https://id.nlm.nih.gov/mesh/sparql")
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
        PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
    
        SELECT ?subject ?p ?pLabel ?o ?oLabel
        FROM <http://id.nlm.nih.gov/mesh>
        WHERE {{
            ?subject rdfs:label "{term}"@en .
            ?subject ?p ?o .
            FILTER(CONTAINS(STR(?p), "concept"))
            OPTIONAL {{ ?p rdfs:label ?pLabel . }}
            OPTIONAL {{ ?o rdfs:label ?oLabel . }}
        }}
        """
        try:
            self.logger.info("Getting alternative MeSH terms...")
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            triples = set()
            for result in results["results"]["bindings"]:
                obj_label = result.get("oLabel", {}).get("value", "No label")
                triples.add(sanitize_term(obj_label))  # Sanitize term before adding

            # Add the sanitized term itself to ensure it's included
            triples.add(sanitize_term(term))
            list_triples = list(triples)
            self.logger.info(f"Got {len(list_triples)} alternative terms.")
            print("\n")
            
            return list_triples

        except Exception as err:
            self.logger.error(f"Error fetching concept triples for term '{term}': {err}")
            raise err

    def __get_narrower_concepts_for_term(self, term: str) -> List[str]:
        term = sanitize_term(term)  # Sanitize input term
        sparql = SPARQLWrapper("https://id.nlm.nih.gov/mesh/sparql")
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
        PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
    
        SELECT ?narrowerConcept ?narrowerConceptLabel
        WHERE {{
            ?broaderConcept rdfs:label "{term}"@en .
            ?narrowerConcept meshv:broaderDescriptor ?broaderConcept .
            ?narrowerConcept rdfs:label ?narrowerConceptLabel .
        }}
        """
        try:
            self.logger.info("Getting Narrower MeSH terms...")
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            concepts = set()
            for result in results["results"]["bindings"]:
                subject_label = result.get("narrowerConceptLabel", {}).get("value", "No label")
                concepts.add(sanitize_term(subject_label))  # Sanitize term before adding
            
            list_concepts = list(concepts)
            self.logger.info(f"Got  {len(list_concepts)} Narrower MeSH terms.")
            
            return list_concepts

        except Exception as err:
            self.logger.error(f"Error fetching narrower concepts for term '{term}': {err}")
            raise err
            


    def __get_all_narrower_concepts(self, term, depth=2, current_depth=1):
        term = sanitize_term(term)  # Sanitize input term
        all_concepts = []
        try:
            narrower_concepts = self.__get_narrower_concepts_for_term(term)
            all_concepts.extend(narrower_concepts)

            if current_depth < depth:
                for concept in narrower_concepts:
                    child_concepts = self.__get_all_narrower_concepts(concept, depth, current_depth + 1)
                    all_concepts.extend(child_concepts)

        except Exception as e:
            print(f"Error fetching all narrower concepts for term '{term}': {e}")

        return all_concepts


    def __query_rdf(self, local_file_path: str, query: str, mesh_terms: List[str], base_namespace:str="http://example.org/mesh/", top_k:int = 10) -> list:
        if not mesh_terms:
            raise ValueError("The list of MeSH terms is empty or invalid.")
        self.logger.info("Querying RDF Knowledge graph...\n\n")
        self.logger.info(f"User's query: {query}")
        sparql_query = """
                                PREFIX schema: <http://schema.org/>
                                PREFIX ex: <http://example.org/>
    
                                SELECT ?article ?title ?abstract ?datePublished ?access ?meshTerm
                                WHERE {{
                                  ?article a ex:Article ;
                                           schema:name ?title ;
                                           schema:description ?abstract ;
                                           schema:datePublished ?datePublished ;
                                           ex:access ?access ;
                                           schema:about ?meshTerm .
    
                                  ?meshTerm a ex:MeSHTerm .
    
                                  
                                }}
                                """

        print("\n\n")
        # Create and parse the RDF graph
        g = Graph()
        g.parse(local_file_path, format="ttl")

        articles_result = self.article_vector_db.query_contexts(query)
        article_uris = [
            result.metadata['article_URI']
            for result in articles_result
            if result.metadata['article_URI']
        ]
        article_uris_string = ", ".join([f"<{str(uri)}>" for uri in article_uris])
        spark_query = sparql_query.format(article_uris=article_uris_string)

        article_data = {}
        self.logger.info("Converting MeSH terms to a valid URI and query on them...")
        for term in mesh_terms:

            mesh_term_uri = convert_to_uri(term, base_namespace)

            results = g.query(spark_query, initBindings={'meshTerm': mesh_term_uri})
            self.logger.info(f"Queried on Term '{term}' and URI: '{mesh_term_uri}'")

            for row in results:
                article_uri = row['article']
                if article_uri not in article_data:
                    article_data[article_uri] = {
                        'title': row['title'],
                        'abstract': row['abstract'],
                        'datePublished': row['datePublished'],
                        'access': row['access'],
                        'meshTerms': set()
                    }
                article_data[article_uri]['meshTerms'].add(str(row['meshTerm']))
            self.logger.info(f"Storaged {len(results)} results.")

        self.logger.info("ranking results...")
        ranked_articles = sorted(
            article_data.items(),
            key=lambda item: len(item[1]['meshTerms']),
            reverse=True
        )
        self.logger.info("Ranked result by number of match MeshTerms.")
        self.logger.info(f"Returned {top_k} ranked results.")
        return ranked_articles[:top_k]

    def get_contexts(self, query: str, graph_path: str, top_k: int = 10) -> list:
        try:
            self.logger.info("Retrieving contexts...")
            mesh_terms_result = self.mesh_terms_vector_db.query_contexts(query)
            retrieved_mesh_terms = [doc.page_content for doc in mesh_terms_result]


            alternative_terms = self.__get_alternative_terms(query)
            narrower_terms = self.__get_all_narrower_concepts(query)
            mesh_terms = []
            mesh_terms.extend(alternative_terms)
            mesh_terms.extend(narrower_terms)
            mesh_terms.extend(retrieved_mesh_terms)
            contexts = self.__query_rdf(graph_path, query, mesh_terms, top_k=top_k)
            self.logger.info(f"Retrieved total top {len(contexts)} contexts successfully !")
            return contexts

        except Exception as err:
            self.logger.error(f"Error during retrieving contexts: \n\n{err}\n\n")
            raise err


if __name__ == "__main__":
    from langchain_openai.embeddings import OpenAIEmbeddings
    from vector_db import PostgresDBVector
    from app_logging import LoggerHandler
    from dotenv import load_dotenv
    from constants import OPENAI_EMBEDDING_MODEL
    import openai
    import os

    GRAPH_PATH = "data/gold/01-06-2025/PubMedGraph.ttl"
    USER_QUERY = 'Emergency'


    load_dotenv(".env")
    openai.api_key = os.environ["OPENAI_API_KEY"]
    openai_embedding = OpenAIEmbeddings(model=OPENAI_EMBEDDING_MODEL)
    logger = LoggerHandler(logger_name="TESTING-HIBRID-SOLUTION-RETRIEVAL", logging_type='console').get_logger()
    retriever = VectorKGRetriever(logger=logger, embeddings=openai_embedding)
    contexts = retriever.get_contexts(USER_QUERY, GRAPH_PATH)



