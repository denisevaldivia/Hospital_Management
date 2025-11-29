import os
import sys


# CASSANDRA ----------------
from cassandra.cluster import Cluster
import modelc
import populatec

CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'logistics')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

def cassandra_menu():
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()
    modelc.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)
    modelc.create_schema(session)

    cassandra_queries = {
        2: modelc.query_1,
        3: modelc.query_2,
        4: modelc.query_3,
        5: modelc.query_4,
        6: modelc.query_5,
        7: modelc.query_7,
        9: modelc.query_8, #esta
        10: modelc.query_9,
        11: modelc.query_10,
        12: modelc.query_11, #esta
        13: modelc.query_12, #esta
        14: modelc.query_13,
        15: modelc.query_14,
    }

    while True:
        print("\n--- CASSANDRA MENU ---")
        print("1. Populate data")
        for i in range(2, 17):
            print(f"{i}. Query {i-1}")
        print("17. Back to main menu")

        choice = int(input("Select an option: "))
        if choice == 1:
            populatec.bulk_insert(session)
        elif 2 <= choice <= 16:
            user_input = input("Enter input for query: ")
            func = cassandra_queries.get(choice)
            if func:
                func(session, user_input)
        elif choice == 17:
            cluster.shutdown()
            break
        else:
            print("Invalid option.")



# MONGODB ----------------
from pymongo import MongoClient
import modelm
import populatem

def mongo_menu():
    client = MongoClient("mongodb://localhost:27017/")
    db = client.Hospital

    mongo_queries = {
        2: modelm.query_1,
        3: modelm.query_2,
        4: modelm.query_3,
        5: modelm.query_4,
        6: modelm.query_5,
        7: modelm.query_6,
        8: modelm.query_7,
        9: modelm.query_8,
        10: modelm.query_9,
        11: modelm.query_10,
        12: modelm.query_11,
        13: modelm.query_12,
        14: modelm.query_13,
        15: modelm.query_14,
        16: modelm.query_15,
    }

    while True:
        print("\n--- MONGODB MENU ---")
        print("1. Populate data")
        for i in range(2, 17):
            print(f"{i}. Query {i-1}")
        print("17. Back to main menu")

        choice = int(input("Select an option: "))
        if choice == 1:
            populatem.populate_data(db)
        elif 2 <= choice <= 16:
            user_input = input("Enter input for query: ")
            func = mongo_queries.get(choice)
            if func:
                func(db, user_input)
            else:
                print("Query not implemented yet")
        elif choice == 17:
            client.close()
            break
        else:
            print("Invalid option.")



# DGRAPH ----------------
import pydgraph
import modeld
import populated

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def dgraph_create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def dgraph_create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def dgraph_close_client_stub(client_stub):
    client_stub.close()

def dgraph_menu():
    client_stub = dgraph_create_client_stub()
    client = dgraph_create_client(client_stub)
    modeld.set_schema(client)

    dgraph_queries = {
        2: modeld.query_1,
        3: modeld.query_2,
        4: modeld.query_3,
        5: modeld.query_4, #esta
        6: modeld.query_5, #esta
        7: modeld.query_6,
        8: modeld.query_7,
        9: modeld.query_8,
        10: modeld.query_9, #esta
        11: modeld.query_10,
        12: modeld.query_11,
        13: modeld.query_12,
        14: modeld.query_13,
        15: modeld.query_14,
    }

    while True:
        print("\n--- DGRAPH MENU ---")
        print("1. Populate data")
        for i in range(2, 17):
            print(f"{i}. Query {i-1}")
        print("17. Back to main menu")

        choice = int(input("Select an option: "))
        if choice == 1:
            populated.populate_data(client)
        elif 2 <= choice <= 16:
            user_input = input("Enter input for query: ")
            func = dgraph_queries.get(choice)
            if func:
                func(client, user_input)
        elif choice == 17:
            dgraph_close_client_stub(client_stub)
            break
        else:
            print("Invalid option.")



# MAIN ----------------
def main():
    while True:
        print("\n=== MAIN MENU ===")
        print("1. Cassandra")
        print("2. MongoDB")
        print("3. Dgraph")
        print("4. Exit")

        choice = input("Select a database: ")
        if choice == "1":
            cassandra_menu()
        elif choice == "2":
            mongo_menu()
        elif choice == "3":
            
            dgraph_menu()
        elif choice == "4":
            print("Exiting application...")
            sys.exit(0)
        else:
            print("Invalid option.")

if __name__ == "__main__":
    main()

