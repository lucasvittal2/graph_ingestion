import re
import random
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from urllib.parse import quote
from rdflib import URIRef
from logging import Logger

def parse_mesh_terms(mesh_list):
    if mesh_list is None:
        return []
    return [term.strip() for term in mesh_list]

def create_valid_uri(base_uri, text):
    if pd.isna(text):
        return None
    sanitized_text = quote(
        text.strip()
        .replace(' ', '_')
        .replace('"', '')
        .replace('<', '')
        .replace('>', '')
        .replace("'", "_")
    )
    return f"{base_uri}/{sanitized_text}"

def convert_to_uri(term, base_namespace="http://example.org/mesh/"):
    """
    Converts a MeSH term into a standardized URI by replacing spaces and special characters with underscores,
    ensuring it starts and ends with a single underscore, and URL-encoding the term.

    Args:
        term (str): The MeSH term to convert.
        base_namespace (str): The base namespace for the URI.

    Returns:
        URIRef: The formatted URI.
    """
    if pd.isna(term):
        return None  # Handle NaN or None terms gracefully

    # Step 1: Strip existing leading and trailing non-word characters (including underscores)
    stripped_term = re.sub(r'^\W+|\W+$', '', term)

    # Step 2: Replace non-word characters with underscores (one or more)
    formatted_term = re.sub(r'\W+', '_', stripped_term)

    # Step 3: Replace multiple consecutive underscores with a single underscore
    formatted_term = re.sub(r'_+', '_', formatted_term)

    # Step 4: URL-encode the term to handle any remaining special characters
    encoded_term = quote(formatted_term)

    # Step 5: Add single leading and trailing underscores
    term_with_underscores = f"_{encoded_term}_"

    # Step 6: Concatenate with base_namespace without adding an extra underscore
    uri = f"{base_namespace}{term_with_underscores}"

    return URIRef(uri)


# Function to generate a random date within the last 5 years
def generate_random_date():
    start_date = datetime.now() - timedelta(days=5 * 365)
    random_days = random.randint(0, 5 * 365)
    return start_date + timedelta(days=random_days)


# Function to generate a random access value between 1 and 10
def generate_random_access():
    return random.randint(1, 10)


# Function to create a valid URI for Articles
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

def sanitize_term(term):
    """
    Clean and format the term:
    - Remove leading/trailing quotes (single or double)
    - Replace underscores with spaces
    - Ensure no unwanted characters remain
    """
    if not term:
        return term
    term = term.strip("'\"")  # Remove single or double quotes
    term = term.replace("_", " ")  # Replace underscores with spaces
    return term.strip()

