import os
import urllib
from utils import *
from logging import Logger
from urllib.parse import quote
from typing import Tuple, List
from rdflib.namespace import SKOS, XSD
from pyspark.sql import SparkSession
from pandas import DataFrame as PandasDataframe
from rdflib import Graph, RDF, RDFS, Namespace, URIRef, Literal
from model import KnowledgeGraphEntities



def start_spark_application(app_name: str, logger: Logger) -> SparkSession:
    logger.info(f"Starting Spark application {app_name}...\n\n")
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    print("\n\n")
    logger.info(f"Spark application {app_name} is now running !")

    return spark


def get_metadata(metadata_path: str, logger: Logger) -> PandasDataframe:
    logger.info(f"Reading metadata from '{metadata_path}'...")
    spark = start_spark_application("read PUBMED articless metadata", logger)
    articles_metadata = spark.read.parquet(metadata_path)
    pandas_df = articles_metadata.toPandas()
    logger.info(f"Got metadata from '{metadata_path}' successfully.")
    return pandas_df

def create_valid_uri(base_uri, text):
    if pd.isna(text):
        return None
    # Encode text to be used in URI
    sanitized_text = urllib.parse.quote(text.strip().replace(' ', '_').replace('"', '').replace('<', '').replace('>', '').replace("'", "_"))
    return URIRef(f"{base_uri}/{sanitized_text}")

def create_article_uri(title, base_namespace="http://example.org/article/"):
    """
    Creates a URI for an article by replacing non-word characters with underscores and URL-encoding.

    Args:
        title (str): The title of the article.
        base_namespace (str): The base namespace for the article URI.

    Returns:
        URIRef: The formatted article URI.
    """
    if pd.isna(title):
        return None
    # Replace non-word characters with underscores
    sanitized_title = re.sub(r'\W+', '_', title.strip())
    # Condense multiple underscores into a single underscore
    sanitized_title = re.sub(r'_+', '_', sanitized_title)
    # URL-encode the term
    encoded_title = quote(sanitized_title)
    # Concatenate with base_namespace without adding underscores
    uri = f"{base_namespace}{encoded_title}"
    return URIRef(uri)

    # Add a new column to the DataFrame for the article URIs
    df['article_uri'] = df['title'].apply(lambda title: create_valid_uri("http://example.org/article", title))
    return df

def setup_knowledge_graph(graph, schema: Namespace, ex:Namespace) -> Tuple[KnowledgeGraphEntities, Graph]:


    prefixes = {
        'schema': schema,
        'ex': ex,
        'skos': SKOS,
        'xsd': XSD
    }
    for p, ns in prefixes.items():
        graph.bind(p, ns)

    # Define classes and properties

    Article = URIRef(ex.Article)
    MeSHTerm = URIRef(ex.MeSHTerm)
    access = URIRef(ex.access)

    #set entities
    graph.add((Article, RDF.type, RDFS.Class))
    graph.add((MeSHTerm, RDF.type, RDFS.Class))

    title = URIRef(schema.name)
    abstract = URIRef(schema.description)
    date_published = URIRef(schema.datePublished)


    graph.add((title, RDF.type, RDF.Property))
    graph.add((abstract, RDF.type, RDF.Property))
    graph.add((date_published, RDF.type, RDF.Property))
    graph.add((access, RDF.type, RDF.Property))

    entities = KnowledgeGraphEntities(
        article=Article,
        mesh_term= MeSHTerm,
        access=access,
        title=title,
        abstract=abstract,
        date_published=date_published
    )
    return entities, graph


def create_rdf_triples(df: pd.DataFrame, graph: Graph, entities: KnowledgeGraphEntities, schema_namespace: Namespace, path_to_save: str = None) -> Graph:
    i = 1
    total_rows = len(df)
    Article = entities.article
    title = entities.title
    abstract = entities.abstract
    MeSHTerm = entities.mesh_term
    access = entities.access
    date_published = entities.date_published
    logger.info("Starting process of building a knowledge graph from dataframe")

    for index, row in df.iterrows():
        article_uri = create_article_uri(row['title'])
        if article_uri is None:
            continue

        # Add Article instance
        graph.add((article_uri, RDF.type, Article))
        graph.add((article_uri, title, Literal(row['title'], datatype=XSD.string)))
        graph.add((article_uri, abstract, Literal(row['abstract'], datatype=XSD.string)))

        # Add random datePublished and access
        random_date = generate_random_date()
        random_access = generate_random_access()
        graph.add((article_uri, date_published, Literal(random_date.date(), datatype=XSD.date)))
        graph.add((article_uri, access, Literal(random_access, datatype=XSD.integer)))

        # Add MeSH Terms
        mesh_terms = parse_mesh_terms(row['meshMajorTerms'])
        for term in mesh_terms:
            term_uri = convert_to_uri(term, base_namespace="http://example.org/mesh/")
            if term_uri is None:
                continue

            # Add MeSH Term instance
            graph.add((term_uri, RDF.type, MeSHTerm))
            graph.add((term_uri, RDFS.label, Literal(term.replace('_', ' '), datatype=XSD.string)))

            # Link Article to MeSH Term
            graph.add((article_uri, schema_namespace.about, term_uri))

        logger.info(f"Added {i} records of {total_rows}")
        i+=1

    logger.info("RDF Knowledge Graph Built successfully.")

    if path_to_save is not None:
        save_graph(path_to_save, logger)

    return graph


def save_graph(path_to_save:str, logger: Logger) -> None:
    if not os.path.exists(path_to_save):
        os.makedirs(path_to_save)
        logger.info(f"Created Save folder '{path_to_save}'.")

    file_path = f"{path_to_save}/PubMedGraph.ttl"
    logger.info(f"Saving graph at {file_path}...")
    graph.serialize(destination=file_path, format='turtle')
    logger.info(f"Knowledge graph saved at {file_path}")

if __name__ == "__main__":
    from datetime import date
    from app_logging import LoggerHandler



    METADATA_PATH = "data/gold/28-05-2025/available_articles_metadata.parquet"

    graph = Graph()
    ex = Namespace('http://example.org/')
    schema = Namespace('http://schema.org/')
    today = date.today().strftime("%d-%m-%Y")
    path_to_save_graph = f"data/gold/{today}"
    logger = LoggerHandler(logger_name="TESTING-GRAPH-BUILDING", logging_type='console').get_logger()
    source_data_df = get_metadata(METADATA_PATH, logger)



    entites, graph = setup_knowledge_graph(graph, schema, ex)
    graph = create_rdf_triples(source_data_df, graph,entites, schema, path_to_save_graph)
