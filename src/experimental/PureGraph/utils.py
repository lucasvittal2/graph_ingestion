import re
import html
import logging
import unicodedata
from typing import List
from langchain_core.documents import Document
from graph_entities import Node, Relationship
from langchain_neo4j.graphs.graph_document import Node as BaseNode, Relationship as BaseRelationship

def format_property_key(s: str) -> str:
    words = s.split()
    if not words:
        return s
    first_word = words[0].lower()
    capitalized_words = [word.capitalize() for word in words[1:]]
    return "".join([first_word] + capitalized_words)

def props_to_dict(props) -> dict:
    """Convert properties to a dictionary."""
    properties = {}
    if not props:
      return properties
    for p in props:
        properties[format_property_key(p.key)] = p.value
    return properties

def map_to_base_node(node: Node) -> BaseNode:
    """Map the KnowledgeGraph Node to the base Node."""
    properties = props_to_dict(node.properties) if node.properties else {}
    # Add name property for better Cypher statement generation
    properties["name"] = node.id.title()
    return BaseNode(
        id=node.id.title(), type=node.type.capitalize(), properties=properties
    )


def map_to_base_relationship(rel: Relationship) -> BaseRelationship:
    """Map the KnowledgeGraph Relationship to the base Relationship."""
    source = map_to_base_node(rel.source)
    target = map_to_base_node(rel.target)
    properties = props_to_dict(rel.properties) if rel.properties else {}
    return BaseRelationship(
        source=source, target=target, type=rel.type, properties=properties
    )

def setup_logs() -> None:
    format = (
        f"[BUILDING-GRAPH-KNOWLEDGE-BASE] - [%(asctime)s] - [%(levelname)s] - %(message)s"
    )
    logging.basicConfig(format=format, level=logging.INFO)


def is_bibliography_section(text: str, threshold: float = 0.7) -> bool:

    if not text or len(text.strip()) < 20:
        return False

    text = text.strip()
    lines = [line.strip() for line in text.split('\n') if line.strip()]

    if not lines:
        return False


    patterns_found = 0
    total_patterns = 8

    numbered_refs = len(re.findall(r'^\d+\.\s+', text, re.MULTILINE))
    if numbered_refs >= 2:
        patterns_found += 1


    author_patterns = len(re.findall(r'\b[A-Z][a-z]+\s+[A-Z]{1,3}\b', text))
    if author_patterns >= 3:
        patterns_found += 1


    journal_year_pattern = len(re.findall(r'\b\d{4};\d+:', text))
    if journal_year_pattern >= 1:
        patterns_found += 1

    page_numbers = len(re.findall(r'\b\d+[-‐]\d+\b', text))
    if page_numbers >= 2:
        patterns_found += 1


    volume_issue = len(re.findall(r'\b\d+:\d+', text))
    if volume_issue >= 2:
        patterns_found += 1

    bib_keywords = ['J ', 'Neurosurg', 'Surgery', 'Med', 'Surg', 'Flow', 'Metab']
    keyword_count = sum(1 for keyword in bib_keywords if keyword in text)
    if keyword_count >= 3:
        patterns_found += 1

    periods_count = text.count('.')
    if periods_count >= len(lines) * 2:  # At least 2 periods per line on average
        patterns_found += 1

    ref_formatting = len(re.findall(r'\.\s+[A-Z][a-z]+\s+[A-Z]', text))
    if ref_formatting >= 2:
        patterns_found += 1

    confidence = patterns_found / total_patterns
    return confidence >= threshold
def sanitize_documents(documents: List[Document]) -> List[Document]:
    sanitized_docs = []
    total_docs =len(documents)
    source = documents[0].metadata["source"]
    num_sanitized = 0

    logging.info(f"Sanitizing documents from '{source}'...\n")
    for i,doc in enumerate(documents):
        doc_cp = doc.model_copy()
        content = doc_cp.page_content

        if (not isinstance(content, str)) or (content=="") or (is_bibliography_section(content)):
            continue

        content = re.sub(r'<[^>]+>', '', content)
        content = html.unescape(content)
        content = unicodedata.normalize('NFKD', content)
        content = re.sub(r'[ \t]+', ' ', content)
        content = re.sub(r'(\n\s*){3,}', '\n\n', content)
        content = content.strip()
        doc_cp.page_content = content
        sanitized_docs.append(doc_cp)
        logging.info(f"Sanitized {i + 1} document of {total_docs} documents.")
        num_sanitized+=1

    logging.info(f"Process completed. Sanitized {num_sanitized} document successfully from '{source}'.\n")
    return sanitized_docs
