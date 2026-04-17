"""
Neo4j aura client

"""

from typing import Any, Dict, List, Optional
from neo4j import GraphDatabase
from loguru import logger


class Neo4jClient:
    """ Thread-safe Neo4j driver wrapper with context-manager support"""
    def __init__(self, uri:str, username: str, password: str, database: str = "neo4j"):
        self.uri = uri
        self._database = database

        self._driver = GraphDatabase.driver(uri, auth=(username, password))

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def query(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute the read query and return list of record dicts"""
        params = params or {}
        with self._driver.session(database=self._database) as session:
            result = session.run(cypher, **params)
            return [dict[str, Any](record) for record in result]


    def write(self, cypher:str, params:Optional[Dict[str, Any]]=None) -> None:
        """ Execute the single write statement """
        params = params or {}
        with self._driver.session(database=self._database) as session:
            session.run(cypher, **params)

        
    def batch_write(self, cypher: str, params_list: List[Dict[str, Any]]) -> int:
        """Excute a write for each param dict in the list using UNWIND.

        The cypher should refrence "$batch" as an UNWIND variable.
        Example:
            UNWIND $batch AS row
            MERGE (d:Drug {name: row.name})
            SET d.description = row.description

        Returns the number of items processed.

        """
        if not params_list:
            return 0
        with self._driver.session(database=self._database) as session:
            session.run(cypher, batch=params_list)
        return len(params_list)


    def get_node_count(self, label: Optional[str] = None) -> int:
        """Count nodes, optionally filtered by label."""
        if label:
            result = self.query(f"MATCH (n:`{label}`) RETURN count(n) AS cnt")
        else:
            result = self.query("MATCH (n) RETURN count(n) AS cnt")
        return result[0]["cnt"] if result else 0

    def get_relationship_count(self, rel_type: Optional[str] = None) -> int:
        """Count relationships, optionally filtered by type."""
        if rel_type:
            result = self.query(f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS cnt")
        else:
            result = self.query("MATCH ()-[r]->() RETURN count(r) AS cnt")
        return result[0]["cnt"] if result else 0

    def get_schema_info(self) -> Dict[str, Any]:
        """Introspect the graph for labels, relationship types, and counts."""
        labels = self.query("CALL db.labels() YIELD label RETURN label")
        rel_types = self.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")

        info = {
            "labels": {},
            "relationship_types": {},
            "total_nodes": self.get_node_count(),
            "total_relationships": self.get_relationship_count(),
        }

        for row in labels:
            lbl = row["label"]
            info["labels"][lbl] = self.get_node_count(lbl)

        for row in rel_types:
            rt = row["relationshipType"]
            info["relationship_types"][rt] = self.get_relationship_count(rt)

        return info


def get_neo4j_client() -> Neo4jClient:
    """Factory: create a Neo4jClient from environment config."""
    from utils.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE

    if not NEO4J_URI:
        raise ValueError("NEO4J_URI is not set. Check your .env file.")
    if not NEO4J_PASSWORD:
        raise ValueError("NEO4J_PASSWORD is not set. Check your .env file.")

    return Neo4jClient(
        uri=NEO4J_URI,
        username=NEO4J_USERNAME,
        password=NEO4J_PASSWORD,
        database=NEO4J_DATABASE,
    )
