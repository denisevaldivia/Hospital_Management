import os
import sys
from datetime import datetime
from cassandra.util import uuid_from_time


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
        6: modelc.query_6,
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
                if choice == 1:  # Prescripción
                    exp = input("Expediente del paciente: ")
                    fecha = uuid_from_time(datetime.now())  # Podrías permitir que el usuario ingrese la fecha
                    doctor = input("Doctor responsable: ")
                    meds = input("Medicamentos indicados: ")
                    obs = input("Observaciones: ")
                    func(session, (exp, fecha, doctor, meds, obs))

                elif choice == 2:  # Historial médico
                    exp = input("Expediente del paciente: ")
                    fecha = uuid_from_time(datetime.now())
                    desc = input("Descripción del diagnóstico: ")
                    obs = input("Observaciones: ")
                    doctor = input("Doctor responsable: ")
                    func(session, (exp, fecha, desc, obs, doctor))

                elif choice == 3:  # Donación de sangre
                    exp = input("Expediente del donador: ")
                    fecha = uuid_from_time(datetime.now())
                    tipo = input("Tipo de sangre: ")
                    nombre = input("Nombre del donador: ")
                    cant = float(input("Cantidad extraída (ml): "))
                    func(session, (exp, fecha, tipo, nombre, cant))

                elif choice == 4:  # Vacuna
                    exp = input("Expediente del paciente: ")
                    fecha = uuid_from_time(datetime.now())
                    tipo = input("Tipo/nombre de la vacuna: ")
                    dosis = input("Dosis administrada: ")
                    lote = input("Lote: ")
                    func(session, (exp, fecha, tipo, dosis, lote))

                elif choice == 5:  # Visita
                    exp = input("Expediente del paciente: ")
                    fecha = uuid_from_time(datetime.now())
                    nombre = input("Nombre del visitante: ")
                    motivo = input("Motivo de la visita: ")
                    dur = int(input("Duración estimada (minutos): "))
                    relacion = input("Relación con el paciente: ")
                    func(session, (exp, fecha, nombre, motivo, dur, relacion))

                elif choice == 6:  # Transacción
                    cuenta = input("Cuenta: ")
                    fecha = uuid_from_time(datetime.now())
                    tipo = input("Tipo de transacción: ")
                    monto = float(input("Monto: "))
                    metodo = input("Método de pago: ")
                    from uuid import uuid4
                    folio = uuid4()
                    func(session, (cuenta, fecha, tipo, monto, metodo, folio))

                elif choice == 7:  # Ocupación de sala
                    sala = input("ID de la sala: ")
                    fecha = uuid_from_time(datetime.now())
                    tipo_sala = input("Tipo de sala: ")
                    estado = input("Estado del evento/procedimiento: ")
                    responsable = input("Responsable: ")
                    desc = input("Descripción del evento: ")
                    func(session, (sala, fecha, tipo_sala, estado, responsable, desc))

                if choice == 8:  # Prescripciones del paciente
                    exp = input("Expediente del paciente: ")
                    inicio = datetime.fromisoformat(i) if (i := input("Fecha inicio (YYYY-MM-DD) o vacío: ")) else None
                    fin = datetime.fromisoformat(i) if (i := input("Fecha fin (YYYY-MM-DD) o vacío: ")) else None
                    func(session, exp, inicio, fin)

                elif choice == 9: # Historial de diagnosticos
                    exp_input = input("Ingrese los expedientes de los pacientes (separados por coma): ")
                    # Convertir a lista y eliminar espacios
                    exp_list = [e.strip() for e in exp_input.split(",") if e.strip()]
                    if exp_list:
                        modelc.query_9(session, exp_list)
                    else:
                        print("No se ingresaron expedientes válidos.")

                elif choice == 10:  # Donaciones
                    exp = input("Expediente del Donador: ")
                    inicio = datetime.fromisoformat(i) if (i := input("Fecha inicio (YYYY-MM-DD) o vacío: ")) else None
                    fin = datetime.fromisoformat(i) if (i := input("Fecha fin (YYYY-MM-DD) o vacío: ")) else None
                    tipo = input("Tipo de sangre o vacío: ") or None
                    func(session, exp, inicio, fin, tipo)

                elif choice == 11:  # Vacunas del paciente
                    exp = input("Expediente del paciente: ")
                    inicio = datetime.fromisoformat(i) if (i := input("Fecha inicio (YYYY-MM-DD) o vacío: ")) else None
                    fin = datetime.fromisoformat(i) if (i := input("Fecha fin (YYYY-MM-DD) o vacío: ")) else None
                    tipo = input("Tipo de vacuna o vacío: ") or None
                    func(session, exp, inicio, fin, tipo)

                elif choice == 12:  # Visitas de un paciente
                    exp = input("Expediente del paciente: ")
                    inicio = datetime.fromisoformat(i) if (i := input("Fecha inicio (YYYY-MM-DD) o vacío: ")) else None
                    fin = datetime.fromisoformat(i) if (i := input("Fecha fin (YYYY-MM-DD) o vacío: ")) else None
                    func(session, exp, inicio, fin)
                
                elif choice == 13: # Transacciones de la cuenta
                    cuenta = input("Ingrese la cuenta a consultar: ")
                    func(session, cuenta)
                
                if choice == 14:  # Logs de la sala
                    id_sala = input("ID Sala: ")
                    inicio = datetime.fromisoformat(i) if (i := input("Fecha inicio (YYYY-MM-DD) o vacío: ")) else None
                    fin = datetime.fromisoformat(i) if (i := input("Fecha fin (YYYY-MM-DD) o vacío: ")) else None
                    func(session, id_sala, inicio, fin)

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
from dgraph_db import modeld,populated2

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
        1: (modeld.query_1, 'Mostrar los doctores que atienden a un paciente'),
        2: (modeld.query_2, 'Mostrar las salas que ha agendado un doctor'),
        3: (modeld.query_3, 'Mostrar los doctores con mayor demanda'),
        4: (modeld.query_4, 'Mostrar las salas de hospital con mayor demanda'),
        5: (modeld.query_5, 'Mostrar los servicios con mayor demanda'), 
        6: (modeld.query_6, 'Consultar cuántos pacientes atiende un doctor'),
        7: (modeld.query_7, 'Consultar el total de recetas que han emitido los doctores según un diagnóstico'),
        8: (modeld.query_8, 'Consultar la ganancia total de cada servicio'),
        9: (modeld.query_9, 'Consultar las visitas de un paciente (y filtrar según el motivo)'),
        10: (modeld.query_10, 'Consultar los diagnósticos que reciben pacientes mayores a 50 años'),
        11: (modeld.query_11, 'Consultar los 3 doctores con mayor experiencia registrados en el hospital'),
    }

    while True:
        print("\n--- DGRAPH MENU ---")
        print("0. Populate data")
        for i in range(1, 12):
            _, desc = dgraph_queries[i]
            print(f"{i}. {desc}")
        print("12. Regresar al menú principal")

        choice = int(input("Selecciona una opción: "))
        # Populate DB
        if choice == 0:
            populated2.populate_data(client)

        # Queries
        elif 1 <= choice <= 11:
            func, _ = dgraph_queries.get(choice)
            if func:
                if choice == 1:
                    id_paciente = input('Ingrese el ID del paciente: ')
                    func(client, id_paciente)
                if choice == 2:
                    id_doctor = input('Ingrese el ID del doctor: ')
                    func(client, id_doctor)
                if choice == 3:
                    func(client)
                if choice == 4:
                    func(client)
                if choice == 5:
                    func(client)
                if choice == 6:
                    id_doctor = input('Ingrese el ID del doctor: ')
                    func(client, id_doctor)

                # Query 10
                if choice == 7:
                    int_attr_dict = {
                        'numbers': [1,2,3,4,5,6],
                        'diagnosis': ['Gripe', 'Alergia', 'Fractura', 'Infección', 'Resfriado', 'Dengue']
                        }
                    
                    # Display choices
                    print('\nDiagnósticos Disponibles:')
                    for i in range(len(int_attr_dict['numbers'])):
                        print(f"{int_attr_dict['numbers'][i]}. {int_attr_dict['diagnosis'][i]}")
                    num_attr = int(input('Selecciona un diagnóstico: '))

                    # Map choice & execute
                    diagnosis = int_attr_dict['diagnosis'][num_attr-1]
                    func(client, diagnosis)

                # Query 11
                if choice == 8:
                    int_attr_dict = {
                        'serv_ids': [1,2,3,4,5,6,7],
                        'serv_names': ['Consulta General', 'Rayos X', 'Análisis de Sangre', 'Vacunación', 'Ultrasonido', 'Terapia Física', 'Tomografía']
                    }

                    # Display choices
                    print('\nServicios Disponibles:')
                    for i in range(len(int_attr_dict['serv_ids'])):
                        print(f"{int_attr_dict['serv_ids'][i]}. {int_attr_dict['serv_names'][i]}")
                    num_attr = int(input('Selecciona un servicio: '))

                    # Map user choice
                    servicio_id = int_attr_dict['serv_ids'][num_attr-1]
                    servicio_name = int_attr_dict['serv_names'][num_attr-1]

                    func(client, str(servicio_id), servicio_name)

                # Query 12
                if choice == 9:
                    visit_motives = [
                        "Visita familiar",
                        "Entrega documentos",
                        "Saludo",
                        "Visita médica",
                        "Entrega comida"
                    ]

                    id_paciente = input('Ingrese el ID del paciente: ')
                    # Visit motives
                    print("\nMotivos de Visita disponibles:")
                    for idx, motive in enumerate(visit_motives, 1):
                        print(f"{idx}. {motive}")
                    # User input for motive
                    filtro = input('Seleccione el motivo de la visita (o presione Enter si no desea filtrar): ')
                    if filtro:
                        motivo = visit_motives[int(filtro) - 1] if filtro.isdigit() and 1 <= int(filtro) <= len(visit_motives) else None
                    # Query
                    func(client, id_paciente, motivo)

                if choice == 10:
                    func(client)
                
                if choice == 11:
                    func(client)

        elif choice == 12:
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
