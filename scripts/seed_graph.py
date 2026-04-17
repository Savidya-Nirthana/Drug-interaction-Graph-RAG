import sys
import json
import argparse
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from utils.neo4j_client import get_neo4j_client

class DrugRecord(BaseModel):
    name: str
    aliases: List[str] = Field(default_factory=list)
    drug_class: str
    description: str
    treats: List[str] = Field(default_factory=list)
    side_effects: List[str] = Field(default_factory=list)
    contraindications: List[str] = Field(default_factory=list)

class InteractionRecord(BaseModel):
    drug_a: str
    drub_b: str
    severity: str
    mechanism: str
    effect: str
    recommendation: str

class DrugInteractionDataset(BaseModel):
    drugs: List[DrugRecord]
    interaction: List[InteractionRecord]
    text_passages: list = Field(default_factory=list)


CONSTRAINT_QUERIES=[
    "CREATE CONSTRAINT drug_name IF NOT EXISTS FOR (d:Drug) REQUIRE d.name IS UNIQUE",
    "CREATE CONSTRAINT condition_name IF NOT EXISTS FOR (c:Condition) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT side_effect_name IF NOT EXISTS FOR (s:SideEffect) REQUIRE s.name IS UNIQUE",
    "CREATE CONSTRAINT drug_class_name IF NOT EXISTS FOR (dc:DrugClass) REQUIRE dc.name IS UNIQUE",
]

INDEX_QUERIES = [
    "CREATE INDEX drug_name_idx IF NOT EXISTS FOR (d:Drug) ON (d.name)",
    "CREATE INDEX condition_name_idx IF NOT EXISTS FOR (c:Condition) ON (c.name)",
    "CREATE INDEX side_effect_name_idx IF NOT EXISTS FOR (s:SideEffect) ON (s.name)",
    "CREATE INDEX drug_class_name_idx IF NOT EXISTS FOR (dc:DrugClass) ON (dc.name)",
]

def main():
    parser = argparse.ArgumentParser(description="Seed Neo4j with drug interaction data")
    parser.add_argument("--clean-first", action="store_true", help="Clean graph before")
    args = parser.parse_args()

    dataset_path = PROJECT_ROOT / "data" / "raw" / "drug_interactions.json"
    print(f"Loading dataset from {dataset_path}...")
    with open(dataset_path) as f:
        raw = json.load(f)
    dataset = DrugInteractionDataset(**raw)

    drug_classes = set(d.drug_class for d in dataset.drugs)
    severities = {}

    for ix in dataset.interactions:
        severities[ix.severity] = severities.get(ix.severity, 0) + 1
    
    print(f" Drugs: {len(dataset.drugs)}")
    print(f" Interactions: {len(dataset.interaction)}")
    print(f" Drug classes: {len(drug_classes)}")
    print(f" Severities: {severities}")

    with get_neo4j_client() as client:
        if not client.verify_connectivity():
            print("failed to connect to neo4j. Check your .env file")
            sys.exit(1)

        if args.clean_first:
            print("\nCleaning graph...")
            client.write("Match (n) DETACH DELETE n")

        for q in CONSTRAINT_QUERIES + INDEX_QUERIES:
            try:
                client.write(q)
            except: 
                pass
        print("schema ready")
    
        drug_batch = [{"name" : d.name, 'description' : d.description, 'aliases' : d.aliases} for d in dataset.drugs]
        client.batch_write("UNWIND $batch AS row MERGE (d:Drug {name: row.name}) SET d.description = row.description, d.aliases = row.aliases", drug_batch)
        print(f"  Created {len(drug_batch)} Drug nodes")

        # 2. Conditions + TREATS
        treats_batch = [{"drug": d.name, "condition": c} for d in dataset.drugs for c in d.treats]
        if treats_batch:
            client.batch_write("UNWIND $batch AS row MERGE (c:Condition {name: row.condition}) MERGE (d:Drug {name: row.drug}) MERGE (d)-[:TREATS]->(c)", treats_batch)
            print(f"  Created {len(set(t['condition'] for t in treats_batch))} Condition nodes, {len(treats_batch)} TREATS edges")

        # 3. SideEffects + CAUSES_SIDE_EFFECT
        se_batch = [{"drug": d.name, "side_effect": se} for d in dataset.drugs for se in d.side_effects]
        if se_batch:
            client.batch_write("UNWIND $batch AS row MERGE (s:SideEffect {name: row.side_effect}) MERGE (d:Drug {name: row.drug}) MERGE (d)-[:CAUSES_SIDE_EFFECT]->(s)", se_batch)
            print(f"  Created {len(set(s['side_effect'] for s in se_batch))} SideEffect nodes, {len(se_batch)} CAUSES_SIDE_EFFECT edges")

        # 4. Contraindications
        contra_batch = [{"drug": d.name, "condition": ci} for d in dataset.drugs for ci in d.contraindications]
        if contra_batch:
            client.batch_write("UNWIND $batch AS row MERGE (c:Condition {name: row.condition}) MERGE (d:Drug {name: row.drug}) MERGE (d)-[:CONTRAINDICATED_FOR]->(c)", contra_batch)
            print(f"  Created {len(contra_batch)} CONTRAINDICATED_FOR edges")

        # 5. DrugClasses + BELONGS_TO_CLASS
        class_batch = [{"drug": d.name, "drug_class": d.drug_class} for d in dataset.drugs]
        if class_batch:
            client.batch_write("UNWIND $batch AS row MERGE (dc:DrugClass {name: row.drug_class}) MERGE (d:Drug {name: row.drug}) MERGE (d)-[:BELONGS_TO_CLASS]->(dc)", class_batch)
            print(f"  Created {len(set(c['drug_class'] for c in class_batch))} DrugClass nodes, {len(class_batch)} BELONGS_TO_CLASS edges")

        # 6. Interactions
        ix_batch = [{"drug_a": ix.drug_a, "drug_b": ix.drug_b, "severity": ix.severity, "mechanism": ix.mechanism, "effect": ix.effect, "recommendation": ix.recommendation} for ix in dataset.interactions]
        if ix_batch:
            client.batch_write("UNWIND $batch AS row MERGE (a:Drug {name: row.drug_a}) MERGE (b:Drug {name: row.drug_b}) MERGE (a)-[r:INTERACTS_WITH]->(b) SET r.severity = row.severity, r.mechanism = row.mechanism, r.effect = row.effect, r.recommendation = row.recommendation, r.source = 'structured'", ix_batch)
            print(f"  Created {len(ix_batch)} INTERACTS_WITH edges")

        # Validation
        info = client.get_schema_info()
        print(f"\nGraph: {info['total_nodes']} nodes, {info['total_relationships']} relationships")
        for label, count in sorted(info["labels"].items()):
            print(f"  {label}: {count}")
        print("\nDone!")


if __name__ == "__main__":
    main()

