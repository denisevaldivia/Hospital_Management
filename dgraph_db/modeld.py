# ------------------------------------
#   DGRAPH MODELO
# ------------------------------------
import pydgraph
import json
import os

def set_schema(client):
    schema = """
        
        id_transaccion: int @index(int) .
        id_paciente: int @index(int) .
        id_sala: int @index(int) .
        id_doctor: int @index(int) .
        id_receta: int @index(int) .

        nombre_servicio: string @index(term) .
        precio: float @index(float) .

        nombre: string .
        sexo: string @index(hash) .
        fecha_nacimiento: dateTime .
        edad: int @index(int) .

        tipo: string @index(term) .
        especialidad: string @index(hash) .
        licencia: string @index(hash) .
        anios_experiencia: int @index(int) .
        correo: string .
        telefono: int .

        nombre_visitante: string .
        relacion_paciente: string @index(hash) .
        motivo: string @index(term) .
        hora_entrada: dateTime @index(hour) .
        hora_salida: dateTime @index(hour) .

        diagnostico: string @index(term) .
        fecha_emision: dateTime @index(day) .
        medicina: string @index(term) .
        cantidad: int .
        frecuencia: int .

        GENERA: uid @reverse .
        SOLICITA: [uid] @reverse .
        RECIBE: [uid] @reverse .
        ATIENDE: [uid] @reverse .
        AGENDA: [uid] @reverse .
        TIENE: [uid] @reverse .
        OTORGA: [uid] @reverse .

        
        type Transaccion {
            id_transaccion
            nombre_servicio
            precio
            GENERA
        }

        type Servicio {
            id_paciente
            nombre_servicio
            GENERA
            SOLICITA
        }

        type Paciente {
            id_paciente
            nombre
            sexo
            fecha_nacimiento
            edad
            ATIENDE
            SOLICITA
            RECIBE
        }

        type Sala {
            id_sala
            id_doctor
            tipo
            AGENDA
        }

        type Doctor {
            id_doctor
            nombre
            especialidad
            licencia
            anios_experiencia
            correo
            telefono
            AGENDA
            ATIENDE
            OTORGA
        }

        type Visita {
            id_paciente
            nombre_visitante
            relacion_paciente
            motivo
            hora_entrada
            hora_salida
            RECIBE
        }

        type Receta {
            id_receta
            id_paciente
            id_doctor
            diagnostico
            fecha_emision
            medicina
            cantidad
            frecuencia
            TIENE
            OTORGA
        }
    """
    op = pydgraph.Operation(schema=schema)
    client.alter(op)
    print("[DGRAPH] Schema configurado correctamente")

# Query 4: Show the relationship between a patient and their doctors
def query_4(client, id_paciente): 
    query = """
    query getDoctors($id_paciente: int) {
        patient(func: eq(id_paciente, $id_paciente)) {
            uid
            paciente_id
            nombre
            sexo
            fecha_nacimiento
            edad
            doctores: ~ATIENDE {
                uid
                id_doctor
                nombre
                especialidad
                licencia
                anios_experiencia
                correo
                telefono
            }
        }
    }
    """

    # Fill out the query
    variables = {"$id_paciente": id_paciente}

    # Execute the response
    txn = client.txn(read_only=True)
    try:
        # Store the response
        res = txn.query(query, variables=variables)
        data = json.loads(res.json)

        # Print the results
        if 'patient' in data:
            for patient in data['patient']:
                print(f"\nQuery 4 Results:")
                print(f"Paciente: {patient.get('nombre')}")
                print(f"Sexo: {patient.get('sexo')}")
                print(f"Fecha de Nacimiento: {patient.get('fecha_nacimiento')}")
                print(f"Edad: {patient.get('edad')}")
                print('Doctores que le han atendido:')
                # Check if 'doctores' exists and is a list
                doctores = patient.get('doctores', [])
                if doctores:
                    for doctor in doctores:
                        print(f"  - Doctor: {doctor.get('nombre')}")
                        print(f"    Especialidad: {doctor.get('especialidad')}")
                        print(f"    Licencia: {doctor.get('licencia')}")
                        print(f"    Años de Experiencia: {doctor.get('anios_experiencia')}")
                        print(f"    Correo: {doctor.get('correo')}")
                        print(f"    Teléfono: {doctor.get('telefono')}")
                else:
                    print("  No doctors found.")
    finally:
        txn.discard()

import json

# Query 5: Show all rooms a doctor has booked
def query_5(client, id_doctor):
    query = """
    query getRooms($id_doctor: int) {
        doctor(func: eq(id_doctor, $id_doctor)) {
            id_doctor
            nombre
            especialidad
            licencia
            anios_experiencia
            correo
            telefono
            salas: AGENDA {
                id_sala
                tipo
            }
        }
    }
    """

    # Define variables for query substitution
    variables = {"$id_doctor": id_doctor}

    # Execute the query
    txn = client.txn(read_only=True)
    try:
        # Query the database
        res = txn.query(query, variables=variables)
        
        # Decode and load the JSON response
        data = json.loads(res.json.decode('utf-8'))
        doctor_data = data.get('doctor', [])

        # Grab the second entry, which contains the full doctor data
        doctor = doctor_data[1] if len(doctor_data) > 1 else None

        if doctor:
            print(f"Doctor: {doctor['nombre']} ({doctor['especialidad']})")
            print(f"Licencia: {doctor['licencia']}")
            print(f"Años de Experiencia: {doctor['anios_experiencia']}")
            print(f"Correo: {doctor['correo']}")
            print(f"Teléfono: {doctor['telefono']}")
            print("Salas agendadas:")

            # Check if the doctor has booked any rooms
            if 'salas' in doctor:
                for sala in doctor['salas']:
                    print(f"  ID Sala: {sala['id_sala']} \n  Tipo: {sala['tipo']}")
            else:
                print("  Ninguna sala ha sido agendada.")
        else:
            print(f"No se encontró información válida para id_doctor {id_doctor}.")
    
    finally:
        txn.discard()

# Query 9: Show number of patients a doctor has
def query_9(client, id_doctor):
    query = """
    query countPatients($id_doctor: string) {
        patient_count(func: type(Paciente)) @filter(has(ATIENDE)) {
            uid
            ATIENDE @filter(eq(id_doctor, $id_doctor)) {
                uid
            }
        }
    }
    """

    # Define variables for query substitution
    variables = {"$id_doctor": id_doctor}

    # Execute the query
    txn = client.txn(read_only=True)
    try:
        # Query the database
        res = txn.query(query, variables=variables)
        
        # Decode and load the JSON response
        data = json.loads(res.json.decode('utf-8'))

        # Extract the list of patient nodes with the specific doctor relationship
        pacientes = data.get('patient_count', [])

        # Print the number of patients related to the doctor
        print(f"Número de Pacientes que atiende el Doctor con ID {id_doctor}: {len(pacientes)}")
    
    finally:
        txn.discard()

# ------------------------------------
#   PRINT RECURSIVE
# ------------------------------------

def pretty_print_recursive(data, indent=0, seen_uids=None):
    if seen_uids is None:
        seen_uids = set()  # Initialize the set to track printed entities by UID

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                # Handle list of entities (like doctors or rooms)
                for item in value:
                    # Check if this item has a UID and whether we've already printed it
                    item_uid = item.get('uid')
                    if item_uid and item_uid not in seen_uids:
                        seen_uids.add(item_uid)  # Mark this item as seen
                        print(f"{' ' * indent}{key.capitalize()}:")  # Print the relationship header (e.g., "Salas:")
                        pretty_print_recursive(item, indent + 2, seen_uids)  # Recursively print the item
            else:
                # For key-value pairs, print normally (skip 'uid')
                if key != 'uid':
                    print(f"{' ' * indent}{key.capitalize()}: {value}")
    elif isinstance(data, list):
        # Process a list of items (e.g., doctors, rooms, etc.)
        for item in data:
            pretty_print_recursive(item, indent, seen_uids)  # No need to check for 'uid' here


# ------------------------------------
#   QUERIES VACÍAS
# ------------------------------------
def query_1(client): pass
def query_2(client): pass
def query_3(client): pass
def query_6(client): pass
def query_7(client): pass
def query_8(client): pass
def query_10(client): pass
def query_11(client): pass
def query_12(client): pass
def query_13(client): pass
def query_14(client): pass

# HACER 4, 5 Y 9