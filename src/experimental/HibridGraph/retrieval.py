from utils import *
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Graph
from typing import List

def get_alternative_terms(term: str) -> List[str]:
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
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        triples = set()
        for result in results["results"]["bindings"]:
            obj_label = result.get("oLabel", {}).get("value", "No label")
            triples.add(sanitize_term(obj_label))  # Sanitize term before adding

        # Add the sanitized term itself to ensure it's included
        triples.add(sanitize_term(term))
        return list(triples)

    except Exception as e:
        print(f"Error fetching concept triples for term '{term}': {e}")
        return []

def get_narrower_concepts_for_term(term: str) -> List[str]:
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
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        concepts = set()
        for result in results["results"]["bindings"]:
            subject_label = result.get("narrowerConceptLabel", {}).get("value", "No label")
            concepts.add(sanitize_term(subject_label))  # Sanitize term before adding

        return list(concepts)

    except Exception as e:
        print(f"Error fetching narrower concepts for term '{term}': {e}")
        return []

# Recursive function to fetch narrower concepts to a given depth
def get_all_narrower_concepts(term, depth=2, current_depth=1):
    term = sanitize_term(term)  # Sanitize input term
    all_concepts = []
    try:
        narrower_concepts = get_narrower_concepts_for_term(term)
        all_concepts.extend(narrower_concepts)

        if current_depth < depth:
            for concept in narrower_concepts:
                child_concepts = get_all_narrower_concepts(concept, depth, current_depth + 1)
                all_concepts.extend(child_concepts)

    except Exception as e:
        print(f"Error fetching all narrower concepts for term '{term}': {e}")

    return all_concepts

def query_rdf(local_file_path: str, query: str, mesh_terms: List[str], base_namespace:str="http://example.org/mesh/") -> list:
    if not mesh_terms:
        raise ValueError("The list of MeSH terms is empty or invalid.")

    print("SPARQL Query:", query)
    SPARQL_QUERY = """
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
    # Create and parse the RDF graph
    g = Graph()
    g.parse(local_file_path, format="ttl")

    articles_result = article_vector_db.query_contexts(query)
    article_uris = [
        result.metadata['article_URI']
        for result in articles_result
        if result.metadata['article_URI']
    ]
    article_uris_string = ", ".join([f"<{str(uri)}>" for uri in article_uris])
    spark_query = SPARQL_QUERY.format(article_uris=article_uris_string)



    article_data = {}
    for term in mesh_terms:
        # Convert the term to a valid URI
        mesh_term_uri = convert_to_uri(term, base_namespace)
        print("Term:", term, "URI:", mesh_term_uri)

        # Perform SPARQL query with initBindings
        results = g.query(spark_query, initBindings={'meshTerm': mesh_term_uri})

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


    ranked_articles = sorted(
        article_data.items(),
        key=lambda item: len(item[1]['meshTerms']),
        reverse=True
    )
    return ranked_articles[:10]


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
    article_vector_db = PostgresDBVector(collection_name="articles", logger=logger, embeddings=openai_embedding)
    mesh_terms_vector_db = PostgresDBVector(collection_name="mesh_terms", logger=logger, embeddings=openai_embedding)



    mesh_terms_result = mesh_terms_vector_db.query_contexts(USER_QUERY)
    retrieved_mesh_terms = [doc.page_content for doc in mesh_terms_result]





    # get narrower and alternative concepts
    alternative_terms = get_alternative_terms(USER_QUERY)
    narrower_terms = get_all_narrower_concepts(USER_QUERY)
    mesh_terms = []
    mesh_terms.extend(alternative_terms)
    mesh_terms.extend(narrower_terms)
    mesh_terms.extend(retrieved_mesh_terms)



    #query on rdf
    contexts = query_rdf(GRAPH_PATH, USER_QUERY, mesh_terms )
    print(contexts)

