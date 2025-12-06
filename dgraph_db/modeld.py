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

        fecha_uso : dateTime .

        nombre_servicio: string @index(term) .
        precio: float @index(float) .
        metodo_pago: string .

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
            SOLICITA
            RECIBE
        }

        type Sala {
            id_sala
            id_doctor
            tipo
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

# Query 7: Hospital Rooms Most Occupied
def query_7(client): 
    query = """
    {
        salas(func: type(Sala)) {
            uid
            id_sala
            tipo
            ocupacion: count(~AGENDA)
        }
    }
    """
    # Execute the query
    txn = client.txn(read_only=True)
    
    try:
        res = txn.query(query)                              # Query
        data = json.loads(res.json.decode('utf-8'))         # Response
    finally:
        txn.discard() 

    # Get all rooms
    salas = data.get("salas", [])
    # Order rooms
    salas = sorted(salas, key=lambda x: x.get("ocupacion", 0), reverse=True)

    print()
    print('=' * 40)
    for sala in salas:
        print(f"Ocupación: {sala.get('ocupacion')} | Tipo de Sala: {sala.get('tipo')} ")
    print('=' * 40)
    print()

# Query 8: Show services sorted by patient demand
def query_8(client): 
    query = """
    {
        servicios(func: type(Servicio)) {
            uid
            id_servicio
            nombre_servicio
            demanda: count(~SOLICITA)
        }
    }
    """
    # Execute the query
    txn = client.txn(read_only=True)
    
    try:
        res = txn.query(query)                              # Query
        data = json.loads(res.json.decode('utf-8'))         # Response
    finally:
        txn.discard() 

    servicios = data.get("servicios", [])
    servicios = sorted(servicios, key=lambda x: x.get("demanda", 0), reverse=True)

    print()
    print('=' * 40)
    for svc in servicios:
        print(f"Demanda: {svc.get('demanda')} | Nombre del Servicio: {svc.get('nombre_servicio')}")    
    print('=' * 40)
    print()

# Query 9: Show number of patients a doctor has
def query_9(client, id_doctor):
    query = """
    query countPatients($id_doctor: int) {
        doctor(func: eq(id_doctor, $id_doctor)) {
            pacientes_count: ATIENDE {
                count(uid)
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

        # Check if there are doctors
        if 'doctor' in data and len(data['doctor']) > 0:
            # Search for patient count
            pacientes_count = data['doctor'][0].get('pacientes_count', [])
            if pacientes_count:
                count = pacientes_count[0].get('count', 0)
                
                # Print results
                print('=' * 40)
                print(f"\nNúmero de Pacientes que atiende el Doctor con ID {id_doctor}: {count}\n")
                print('=' * 40)
            else:
                print(f"No se encontró el número de pacientes para el Doctor con ID {id_doctor}.")
        else:
            print(f"No se encontró al Doctor con ID {id_doctor}.")

    finally:
        txn.discard()

# Query 10: Number of prescriptions doctors have given by diagnosis
def query_10(client, diagnosis): 
    query = """
    query countPrescriptions($diagnosis: string) {
        recetas(func: eq(diagnostico, $diagnosis)) {
            id_receta
            doctor_name: ~OTORGA {
                id_doctor
                nombre
            }
        }
    }
    """

    # Define variables for query substitution
    variables = {"$diagnosis": diagnosis}

    # Execute the query
    txn = client.txn(read_only=True)
    try:
        # Query the database
        res = txn.query(query, variables=variables)

        # Store the response
        data = json.loads(res.json.decode('utf-8'))

        # Check if there are any prescriptions
        if 'recetas' in data and len(data['recetas']) > 0:
            print()
            print('=' * 40)
            print(f"\nNúmero de Recetas para diagnóstico: {diagnosis}\n")
            
            # Create a dictionary to count recipes per doctor
            doctor_recipes_count = {}

            # Loop through the recipes and count the prescriptions per doctor
            for receta in data['recetas']:
                if receta.get('doctor_name'):
                    doctor_name = receta['doctor_name'][0].get('nombre', 'Desconocido')
                    # Increment the recipe count for that doctor
                    if doctor_name in doctor_recipes_count:
                        doctor_recipes_count[doctor_name] += 1
                    else:
                        doctor_recipes_count[doctor_name] = 1

            # Print the result for each doctor
            for doctor_name, count in doctor_recipes_count.items():
                print(f"Doctor: {doctor_name}, Número de diagnósticos para {diagnosis}: {count}")

            print()
            print('=' * 40)
        else:
            print(f"No se encontraron recetas con el diagnóstico '{diagnosis}'.")
    
    finally:
        txn.discard()

#! REVISITAR
# Query 11: Transactions by service + total amount
def query_11(client, id_servicio, name): 
    query = """
    {
        servicio(func: eq(id_servicio, $id_servicio)) {
            id_servicio
            nombre_servicio
            GENERA {
                uid
                id_transaccion
                nombre_servicio
                precio
            }
        }
    }
    """

    # Fill out the query
    variables = {"$id_servicio": id_servicio}

    # Execute the query
    txn = client.txn(read_only=True)
    try:
        # Store the response
        res = txn.query(query, variables=variables)
        data = json.loads(res.json)

        # Get transactions
        servicio = data.get('servicio', [])[0]  
        transactions = servicio.get('GENERA', [])

        # Calculate total earnings
        if transactions: 
            total_precio = sum(transaction.get('precio', 0.0) for transaction in transactions)
        else:
            total_precio = 0.0
        

        # Print the results
        print()
        print('=' * 40)
        print(f'Total de Transacciones para Servicio {name}:\n')
        print(f'\nGanancia Total de las transacciones para el Servicio {name}: {total_precio}')
        print('=' * 40)

    finally:
        txn.discard()



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


def query_12(client): pass
def query_13(client): pass
def query_14(client): pass