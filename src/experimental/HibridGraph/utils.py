import re
import random
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
from rdflib import URIRef


def parse_mesh_terms(mesh_list):
    if mesh_list is None:
        return []
    return [term.strip() for term in mesh_list]


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
def create_article_uri(title, base_namespace="http://example.org/article"):
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
    # Encode text to be used in URI
    sanitized_text = quote(
        title.strip().replace(' ', '_').replace('"', '').replace('<', '').replace('>', '').replace("'", "_"))
    return URIRef(f"{base_namespace}/{sanitized_text}")