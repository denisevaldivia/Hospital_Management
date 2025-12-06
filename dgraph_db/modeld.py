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
        id_visita: int @index(int) .
        id_servicio: int @index(int) .

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
            id_servicio
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
            id_visita
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
        patient(func: eq(id_paciente, $id_paciente)) @filter(has(nombre)) {
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
        print()
        print('=' * 40)
        print('Doctores que han atendido a un Paciente:\n')
        if 'patient' in data:
            found_patient = False                    # No patient has been found
            for patient in data['patient']:
                found_patient = True                 # A patient has been found
                # Attributes of Patient nodes
                for field in ['nombre', 'sexo', 'fecha_nacimiento', 'edad']:
                    check_before_print(patient, field)
                
                # Doctors
                print('Doctores que le han atendido:')
                doctores = patient.get('doctores', [])

                # Check if 'doctores' exists and is a list
                if doctores:
                    for index, doctor in enumerate(doctores):
                        if index > 0:
                            print()
                        for field in ['nombre', 'especialidad', 'licencia', 'anios_experiencia', 'correo', 'telefono']:
                            check_before_print(doctor, field, indent_level=1)
                else:
                    print("  No se encontraron doctores.")
            if not found_patient:
                print(f'No se ha encontrado un Paciente con ID {id_paciente}.')
        print('=' * 40)

    finally:
        txn.discard()

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
        # Store the response
        res = txn.query(query, variables=variables)
        data = json.loads(res.json)

        # Print the results
        print()
        print('=' * 40)
        print('Salas agendadas por Doctor:\n')

        if 'doctor' in data:
            found_doctor = False                # No doctor found
            found_room = False                  # No room found
            for doctor in data['doctor']:
                found_doctor = True             # Doctor found

                # Print Doctor
                for field in ['nombre', 'especialidad', 'licencia', 'anios_experiencia', 'correo', 'telefono']:
                    check_before_print(doctor, field)

                # Check if the doctor has booked any rooms
                salas = doctor.get('salas', [])
                
                # Print Rooms
                if salas:
                    found_room = True
                    print("Salas agendadas:\n")
                    for index, sala in enumerate(salas):
                        if index > 0:
                            print()
                        for field in ['id_sala', 'tipo']:
                            check_before_print(sala, field, indent_level=1)
                if not found_room:
                    print("  No se han agendado salas.")
                
            if not found_doctor:
                print(f'No se ha encontrado un Doctor con ID {id_doctor}.')
        print('=' * 40)
    
    finally:
        txn.discard()

# Query 9: Show number of patients a doctor has
def query_9(client, id_doctor):
    query = """
    query countPatients($id_doctor: string) {
        patient_count(func: type(Paciente)) @filter(has(nombre)) {
            uid
            nombre
            ~ATIENDE @filter(eq(id_doctor, $id_doctor)) {
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
        
        # Store the response
        data = json.loads(res.json)

        # Extract the list of patient nodes with the specific doctor relationship
        pacientes = data.get('patient_count', [])

        # Filter valid patients
        valid_patients = [p for p in pacientes if any(d['id_doctor'] == id_doctor for d in p.get('~ATIENDE', []))]

        # Print the number of patients related to the doctor
        print('=' * 40)
        print(f"\nNúmero de Pacientes que atiende el Doctor con ID {id_doctor}: {len(valid_patients)}\n")
        print('=' * 40)

    finally:
        txn.discard()

# Query 10: Number of prescriptions doctors have given by diagnosis
def query_10(client): 
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

# ------------------------------------
#   CHECK AND PRINT
# ------------------------------------

def check_before_print(data, field_name, indent_level=0):
    value = data.get(field_name)
    # Only print if values exist
    if value:
        indent = '    ' * indent_level             # Indentation
        print(f"{indent}{field_name.replace('_', ' ').capitalize()}: {value}")

# ------------------------------------
#   QUERIES VACÍAS
# ------------------------------------
def query_1(client): pass
def query_2(client): pass
def query_3(client): pass
def query_6(client): pass
def query_7(client): pass
def query_8(client): pass
def query_11(client): pass
def query_12(client): pass
def query_13(client): pass
def query_14(client): pass

# HACER 4, 5 Y 9