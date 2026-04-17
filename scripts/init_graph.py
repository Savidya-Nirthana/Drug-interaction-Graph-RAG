import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT/'.env')


from utils.neo4j_client import get_neo4j_client

CONSTRAINT_QUERIES=[
    "CREATE CONSTRAINT drug_name IF NOT EXISTS FOR (d:Drug) REQUIRE d.name IS UNIQUE",
    "CREATE CONSTRAINT condition_name IF NOT EXISTS FOR (c:Condition) REQUIRE c.name IS UNIQUE",
    "CREATE CONSTRAINT side_effect_name IF NOT EXISTS FOR (s:SideEffect) REQUIRE s.name IS UNIQUE",
    "CREATE CONSTRAINT drug_class_name IF NOT EXISTS FOR (dc:DrugClass) REQUIRE dc.name IS UNIQUE",
]

INDEX_QUERIES=[
    "CREATE INDEX drug_name_idx IF NOT EXISTS FOR (d:Drug) ON (d.name)",
    "CREATE INDEX condition_name_idx IF NOT EXISTS FOR (c:Condition) ON (c.name)",
    "CREATE INDEX side_effect_name_idx IF NOT EXISTS FOR (s:SideEffect) ON (s.name)",
    "CREATE INDEX drug_class_name_idx IF NOT EXISTS FOR (dc:DrugClass) ON (dc.name)",
]

def main():
    parser = argparse.ArgumentParser(description="Initialize Neo4j graph schema")
    parser.add_argument("--clean", action="store_true", help="Delete all node and relationships first")
    args = parser.parse_args()

    with get_neo4j_client() as client:
        if not client.verify_connectivity():
            print("Failed to connect to Neo4j. Check your .env file. ")
            sys.exit(1)
        
        if args.clean:
            print("Cleaning graph - deleting all nodes and relationships...")
            client.write("Match (n) DETACH DELETE n")
            print("Graph cleaned.")

        print("creating constraints...")


        for q in CONSTRAINT_QUERIES:
            try:
                client.write(q)
            except Exception as e:
                pass
        
        print("Creating indexes...")
        for q in INDEX_QUERIES:
            try:
                client.write(q)
            except Exception as e:
                pass
        
        print("Schema setup complete!")

        info  = client.get_schema_info()
        print(f"\nGraph state: {info['total_nodes']} nodes, {info['total_relationships']} relationships")

if __name__ == "__main__":
    main()

        
        
