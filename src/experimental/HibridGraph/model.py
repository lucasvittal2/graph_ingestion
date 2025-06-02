from dataclasses import dataclass

from rdflib import URIRef

@dataclass
class KnowledgeGraphEntities:
    article: URIRef
    mesh_term: URIRef
    access: URIRef
    title: URIRef
    abstract: URIRef
    date_published: URIRef
    access: URIRef
