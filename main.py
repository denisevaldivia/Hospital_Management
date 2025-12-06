import os
import sys
from datetime import datetime


# ------------------------------------
#   CASSANDRA 
# ------------------------------------
from cassandra.cluster import Cluster
from cassandra_db import modelc, populatec                # This can be aliased --> as <name>

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
        1: modelc.query_1,
        2: modelc.query_2,
        3: modelc.query_3,
        4: modelc.query_4,
        5: modelc.query_5,
        7: modelc.query_7,
        8: modelc.query_8, 
        9: modelc.query_9,
        10: modelc.query_10,
        11: modelc.query_11, 
        12: modelc.query_12,
        13: modelc.query_13,
        14: modelc.query_14,
    }

    while True:
        print("\n--- CASSANDRA MENU ---")
        print("0. Populate data")
        for i in range(1, 15):
            print(f"{i}. Query {i}")
        print("15. Back to main menu")
        print("404. Drop all tables")

        choice = int(input("Select an option: "))
        if choice == 0:
            populatec.bulk_insert(session)
        elif 1 <= choice <= 14:
            func = cassandra_queries.get(choice)
            if func:
                if choice == 8:  # Query 8
                    exp = input("Expediente del paciente: ")
                    inicio = datetime.fromisoformat(i) if (i := input("Fecha inicio (YYYY-MM-DD) o vacío: ")) else None
                    fin = datetime.fromisoformat(i) if (i := input("Fecha fin (YYYY-MM-DD) o vacío: ")) else None
                    func(session, exp, inicio, fin)

                elif choice == 11:  # Query 11
                    exp = input("Expediente del paciente: ")
                    inicio = datetime.fromisoformat(i) if (i := input("Fecha inicio (YYYY-MM-DD) o vacío: ")) else None
                    fin = datetime.fromisoformat(i) if (i := input("Fecha fin (YYYY-MM-DD) o vacío: ")) else None
                    tipo = input("Tipo de vacuna o vacío: ") or None
                    func(session, exp, inicio, fin, tipo)

                elif choice == 12:  # Query 12
                    exp = input("Expediente del paciente: ")
                    inicio = datetime.fromisoformat(i) if (i := input("Fecha inicio (YYYY-MM-DD) o vacío: ")) else None
                    fin = datetime.fromisoformat(i) if (i := input("Fecha fin (YYYY-MM-DD) o vacío: ")) else None
                    func(session, exp, inicio, fin)
                    # func(session)
        elif choice == 15:
            cluster.shutdown()
            break
        elif choice == 404:
            modelc.drop_schema(session)
            break
        else:
            print("Invalid option.")

# ------------------------------------
#   MONGODB
# ------------------------------------
from pymongo import MongoClient
from mongo_db import modelm, populatem

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
    }

    while True:
        print("\n--- MONGODB MENU ---")
        print("1. Populate data")
        for i in range(2, 14):
            print(f"{i}. Query {i-1}")
        print("14. Back to main menu")

        choice = int(input("Select an option: "))
        if choice == 1:
            populatem.populate_data(db)
        elif 2 <= choice <= 13:
            func = mongo_queries.get(choice)
            if func:
                func(db)
            else:
                print("Query not implemented yet")
        elif choice == 14:
            client.close()
            break
        else:
            print("Invalid option.")



# ------------------------------------
#   DGRAPH
# ------------------------------------
import pydgraph
from dgraph_db import modeld, populated, populated2, populate3

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def dgraph_create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def dgraph_create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def dgraph_menu():
    client_stub = dgraph_create_client_stub()
    client = dgraph_create_client(client_stub)
    modeld.set_schema(client)

    dgraph_queries = {
        1: (modeld.query_1, 'Registrar a un nuevo doctor'),
        2: (modeld.query_2, 'Registrar a un nuevo paciente'),
        3: (modeld.query_3, 'Registrar a una visita'),
        4: (modeld.query_4, 'Mostrar los doctores que atienden a un paciente'),
        5: (modeld.query_5, 'Mostrar las salas que ha agendado un doctor'), 
        6: (modeld.query_6, 'Mostrar los doctores con mayor demanda'),
        7: (modeld.query_7, 'Mostrar las salas de hospital con mayor demanda'),
        8: (modeld.query_8, 'Mostrar los servicios con mayor demanda'),
        9: (modeld.query_9, 'Consultar cuántos pacientes atiende un doctor'),
        10: (modeld.query_10, 'Consultar el total de recetas que han emitido los doctores según un diagnóstico'),
        11: (modeld.query_11, 'Consultar la ganancia total de cada servicio'),
        12: (modeld.query_12, 'Consultar las visitas de un paciente (y filtrar según el motivo)'),
        13: (modeld.query_13, 'Consultar los diagnósticos que reciben pacientes mayores a 50 años'),
        14: (modeld.query_14, 'Consultar los 3 doctores con mayor experiencia registrados en el hospital'),
    }

    while True:
        print("\n--- DGRAPH MENU ---")
        print("0. Populate data")
        for i in range(1, 15):
            _, desc = dgraph_queries[i]
            print(f"{i}. {desc}")
        print("15. Regresar al menú principal")

        choice = int(input("Selecciona una opción: "))
        # Populate DB
        if choice == 0:
            populate3.populate_data(client)

        # Queries
        elif 1 <= choice <= 14:
            func, _ = dgraph_queries.get(choice)
            if func:
                if choice == 4:
                    id_paciente = input('Ingrese el ID del paciente: ')
                    func(client, id_paciente)
                if choice == 5:
                    id_doctor = input('Ingrese el ID del doctor: ')
                    func(client, id_doctor)
                if choice == 9:
                    id_doctor = input('Ingrese el ID del doctor: ')
                    func(client, id_doctor)
        elif choice == 15:
            client_stub.close()
            break
        else:
            print("Invalid option.")


# ------------------------------------
#   MAIN
# ------------------------------------

def main():
    while True:
        print("\n=== MAIN MENU ===")
        print("1. Cassandra (Logs)")
        print("2. MongoDB (Catálogos)")
        print("3. Dgraph (Relaciones)")
        print("4. Exit")

        choice = input("Select a database: ")
        if choice == "1":
            cassandra_menu()
        elif choice == "2":
            mongo_menu()
        elif choice == "3":
            
            dgraph_menu()
        elif choice == "4":
            print("Exiting application of Ganó el Amor...")
            sys.exit(0)
        else:
            print("Invalid option.")

if __name__ == "__main__":
    main()


