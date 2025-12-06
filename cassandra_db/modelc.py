# ------------------------------------
#   CASSANDRA MODELO
# ------------------------------------

def create_keyspace(session, keyspace, replication_factor):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': {replication_factor}
        }};
    """)
    print("Keyspace creado con éxito en Cassandra")


def create_schema(session):
    # Prescriptions by patient
    session.execute("""
        CREATE TABLE IF NOT EXISTS prescriptions_by_patient (
            exp_paciente text,
            fecha_creacion timeuuid,
            doctor_responsable text,
            medicamentos text,
            observaciones text,
            PRIMARY KEY ((exp_paciente), fecha_creacion)
        ) WITH CLUSTERING ORDER BY (fecha_creacion ASC);
    """)

    # Logs by transactions
    session.execute("""
        CREATE TABLE IF NOT EXISTS logs_by_transactions (
            cuenta text,
            fecha_pago timeuuid,
            tipo_transaccion text,
            monto float,
            metodo_pago text,
            folio uuid,
            PRIMARY KEY ((cuenta), fecha_pago, tipo_transaccion)
        ) WITH CLUSTERING ORDER BY (fecha_pago ASC);
    """)

    # Blood donation by patient
    session.execute("""
        CREATE TABLE IF NOT EXISTS blood_donation_by_patient (
            exp_donador text,
            fecha_donacion timeuuid,
            tipo_sangre text,
            nombre text,
            cantidad float,
            PRIMARY KEY ((exp_donador), fecha_donacion, tipo_sangre)
        ) WITH CLUSTERING ORDER BY (fecha_donacion ASC);
    """)

    # Vaccines by patient
    session.execute("""
        CREATE TABLE IF NOT EXISTS vaccines_by_patient (
            exp_paciente text,
            fecha_vacuna timeuuid,
            tipo_vacuna text,
            dosis text,
            lote text,
            PRIMARY KEY ((exp_paciente), fecha_vacuna)
        ) WITH CLUSTERING ORDER BY (fecha_vacuna ASC);
    """)

    # Logs by visit
    session.execute("""
        CREATE TABLE IF NOT EXISTS logs_by_visit (
            exp_paciente text,
            fecha_visita timeuuid,
            nombre_visitante text,
            motivo text,
            duracion int,
            relacion_paciente text,
            PRIMARY KEY ((exp_paciente), fecha_visita)
        ) WITH CLUSTERING ORDER BY (fecha_visita ASC);
    """)

    # Historial by patient
    session.execute("""
        CREATE TABLE IF NOT EXISTS historial_by_patient (
            exp_paciente text,
            fecha_diagnostico timeuuid,
            descripcion text,
            observaciones text,
            doctor_responsable text,
            PRIMARY KEY ((exp_paciente), fecha_diagnostico)
        ) WITH CLUSTERING ORDER BY (fecha_diagnostico ASC);
    """)

    # Logs by room
    session.execute("""
        CREATE TABLE IF NOT EXISTS logs_by_room (
            id_sala text,
            fecha_evento timeuuid,
            tipo_sala text,
            estado text,
            responsable text,
            descripcion text,
            PRIMARY KEY ((id_sala), fecha_evento)
        ) WITH CLUSTERING ORDER BY (fecha_evento ASC);
    """)

    print("Schema creado con éxito en Cassandra")

# ------------------------------------
#   DROPEAR TODAS LAS TABLAS
# ------------------------------------
def drop_schema(session):

    tables = [
        "prescriptions_by_patient",
        "logs_by_transactions",
        "blood_donation_by_patient",
        "vaccines_by_patient",
        "logs_by_visit",
        "historial_by_patient",
        "logs_by_room"
    ]
    for table in tables:
        session.execute(f"DROP TABLE IF EXISTS {table}")

    print("All tables dropped successfully!")

# ------------------------------------
#   QUERIES VACÍAS
# ------------------------------------

def query_1(session, params):
    query = """INSERT INTO prescriptions_by_patient 
            (exp_paciente, fecha_creacion, doctor_responsable, medicamentos, observaciones) 
            VALUES (?, ?, ?, ?, ?)"""

    # Preparar y ejecutar
    stmt = session.prepare(query)
    rows = session.execute(stmt, params)

    # Mostrar resultados
    print(f"\n=== Se añadío la nueva prescripción con éxito ===")

def query_2(session, params):
    query = """INSERT INTO historial_by_patient 
               (exp_paciente, fecha_diagnostico, descripcion, observaciones, doctor_responsable) 
               VALUES (?, ?, ?, ?, ?)"""
    
    stmt = session.prepare(query)
    session.execute(stmt, params)
    print("\n=== Se añadió el nuevo diagnóstico/historial médico con éxito ===")

def query_3(session, params):
    query = """INSERT INTO blood_donation_by_patient 
               (exp_donador, fecha_donacion, tipo_sangre, nombre, cantidad) 
               VALUES (?, ?, ?, ?, ?)"""
    
    stmt = session.prepare(query)
    session.execute(stmt, params)
    print("\n=== Se añadió la nueva donación de sangre con éxito ===")

def query_4(session, params):
    query = """INSERT INTO vaccines_by_patient 
               (exp_paciente, fecha_vacuna, tipo_vacuna, dosis, lote) 
               VALUES (?, ?, ?, ?, ?)"""
    
    stmt = session.prepare(query)
    session.execute(stmt, params)
    print("\n=== Se añadió la nueva vacuna con éxito ===")

def query_5(session, params):
    query = """INSERT INTO logs_by_visit 
               (exp_paciente, fecha_visita, nombre_visitante, motivo, duracion, relacion_paciente) 
               VALUES (?, ?, ?, ?, ?, ?)"""
    
    stmt = session.prepare(query)
    session.execute(stmt, params)
    print("\n=== Se añadió la nueva visita con éxito ===")

def query_6(session, params):
    query = """INSERT INTO logs_by_transactions 
               (cuenta, fecha_pago, tipo_transaccion, monto, metodo_pago, folio) 
               VALUES (?, ?, ?, ?, ?, ?)"""
    
    stmt = session.prepare(query)
    session.execute(stmt, params)
    print("\n=== Se añadió la nueva transacción con éxito ===")

def query_7(session, params):
    query = """INSERT INTO logs_by_room 
               (id_sala, fecha_evento, tipo_sala, estado, responsable, descripcion) 
               VALUES (?, ?, ?, ?, ?, ?)"""
    
    stmt = session.prepare(query)
    session.execute(stmt, params)
    print("\n=== Se añadió el nuevo registro de sala con éxito ===")

def query_8(session, exp_paciente, inicio=None, fin=None):
    # Base de la query
    query = """
        SELECT exp_paciente, toDate(fecha_creacion) AS order_date_readable,
               doctor_responsable, medicamentos, observaciones
        FROM prescriptions_by_patient
        WHERE exp_paciente = ?
    """

    params = [exp_paciente]

    # Agrega filtros opcionales
    if inicio is not None:
        query += " AND fecha_creacion >= minTimeuuid(?)"
        params.append(inicio)

    if fin is not None:
        query += " AND fecha_creacion <= maxTimeuuid(?)"
        params.append(fin)

    # Preparar y ejecutar
    stmt = session.prepare(query)
    rows = session.execute(stmt, params)

    # Mostrar resultados
    print(f"\n=== Prescripciones del paciente: {exp_paciente} ===")
    if inicio or fin:
        print(f"Rango: {inicio} → {fin}\n")

    for row in rows:
        print(f"\nPaciente: {row.exp_paciente}")
        print(f"  - Fecha: {row.order_date_readable}")
        print(f"  - Doctor Responsable: {row.doctor_responsable}")
        print(f"  - Medicamentos: {row.medicamentos}")
        print(f"  - Observaciones: {row.observaciones}")
        print()

def query_9(session, exp_pacientes):
    # Asegurarnos de que exp_pacientes sea lista
    if isinstance(exp_pacientes, str):
        exp_pacientes = [exp_pacientes]

    # Construir placeholders para IN
    placeholders = ", ".join(["?"] * len(exp_pacientes))

    query = f"""
        SELECT exp_paciente, toDate(fecha_diagnostico) AS fecha_readable,
               descripcion, observaciones, doctor_responsable
        FROM historial_by_patient
        WHERE exp_paciente IN ({placeholders})
    """

    stmt = session.prepare(query)
    rows = session.execute(stmt, tuple(exp_pacientes))

    print(f"\n=== Historial médico de los pacientes: {exp_pacientes} ===")
    for row in rows:
        print(f"\nPaciente: {row.exp_paciente}")
        print(f"  - Fecha diagnóstico: {row.fecha_readable}")
        print(f"  - Descripción: {row.descripcion}")
        print(f"  - Observaciones: {row.observaciones}")
        print(f"  - Doctor responsable: {row.doctor_responsable}")

def query_10(session, exp_donador, inicio=None, fin=None, tipo=None):
    # Base de la query
    query = """
        SELECT exp_donador, toDate(fecha_donacion) AS order_date_readable,
               tipo_sangre, nombre, cantidad
        FROM blood_donation_by_patient
        WHERE exp_donador = ?
    """

    params = [exp_donador]

    # Agrega filtros opcionales
    if inicio is not None:
        query += " AND fecha_donacion >= minTimeuuid(?)"
        params.append(inicio)

    if fin is not None:
        query += " AND fecha_donacion <= maxTimeuuid(?)"
        params.append(fin)

    # Preparar y ejecutar
    stmt = session.prepare(query)
    rows = session.execute(stmt, params)

    # Mostrar resultados
    print(f"\n=== Donaciones realizadas por: {exp_donador} ===")
    if inicio or fin:
        print(f"Rango: {inicio} → {fin}\n")
    
    for row in rows:
        # Filtro por tipo manual
        if tipo is not None and row.tipo_sangre != tipo:
            continue

        print(f"\nDonador: {row.exp_donador}")
        print(f"  - Fecha: {row.order_date_readable}")
        print(f"  - Tipo: {row.tipo_sangre}")
        print(f"  - Nombre: {row.nombre}")
        print(f"  - Cantidad: {row.cantidad}")
        print()

def query_11(session, exp_paciente, inicio=None, fin=None, tipo=None):
    # Base de la query
    query = """
        SELECT exp_paciente, toDate(fecha_vacuna) AS order_date_readable,
               tipo_vacuna, dosis, lote
        FROM vaccines_by_patient
        WHERE exp_paciente = ?
    """

    params = [exp_paciente]

    # Agrega filtros opcionales
    if inicio is not None:
        query += " AND fecha_vacuna >= minTimeuuid(?)"
        params.append(inicio)

    if fin is not None:
        query += " AND fecha_vacuna <= maxTimeuuid(?)"
        params.append(fin)

    # Preparar y ejecutar
    stmt = session.prepare(query)
    rows = session.execute(stmt, params)

    # Mostrar resultados
    print(f"\n=== Vacunas del paciente: {exp_paciente} ===")
    if inicio or fin:
        print(f"Rango: {inicio} → {fin}\n")
    
    for row in rows:
        # Filtro por tipo manual
        if tipo is not None and row.tipo_vacuna != tipo:
            continue

        print(f"\nPaciente: {row.exp_paciente}")
        print(f"  - Fecha: {row.order_date_readable}")
        print(f"  - Tipo: {row.tipo_vacuna}")
        print(f"  - Dosis: {row.dosis}")
        print(f"  - Lote: {row.lote}")
        print()

def query_12(session, exp_paciente, inicio=None, fin=None):
    # Base de la query
    query = """
        SELECT exp_paciente, toDate(fecha_visita) AS order_date_readable,
               nombre_visitante, motivo, duracion, relacion_paciente
        FROM logs_by_visit
        WHERE exp_paciente = ?
    """

    params = [exp_paciente]

    # Agrega filtros opcionales
    if inicio is not None:
        query += " AND fecha_visita >= minTimeuuid(?)"
        params.append(inicio)

    if fin is not None:
        query += " AND fecha_visita <= maxTimeuuid(?)"
        params.append(fin)
    
    # Preparar y ejecutar
    stmt = session.prepare(query)
    rows = session.execute(stmt, params)

    # Mostrar resultados
    print(f"\n=== Visitas al paciente: {exp_paciente} ===")
    if inicio or fin:
        print(f"Rango: {inicio} → {fin}\n")

    for row in rows:
        print(f"\nPaciente: {row.exp_paciente}")
        print(f"  - Fecha: {row.order_date_readable}")
        print(f"  - Nombre del Visitante: {row.nombre_visitante}")
        print(f"  - Relación: {row.relacion_paciente}")
        print(f"  - Motivo: {row.motivo}")
        print(f"  - Duracion: {row.duracion}")
        print()

def query_13(session, cuenta):
    query = f"""
        SELECT cuenta, toDate(fecha_pago) AS fecha_readable,
               tipo_transaccion, monto, metodo_pago, folio
        FROM logs_by_transactions
        WHERE cuenta = ?
    """

    stmt = session.prepare(query)
    rows = session.execute(stmt, (cuenta,))

    print(f"\n=== Transacciones de la cuenta: {cuenta} ===")
    for row in rows:
        print(f"\nCuenta: {row.cuenta}")
        print(f"  - Fecha Pago: {row.fecha_readable}")
        print(f"  - Tipo: {row.tipo_transaccion}")
        print(f"  - Monto: {row.monto}")
        print(f"  - Método de pago: {row.metodo_pago}")
        print(f"  - Folio: {row.folio}")

def query_14(session, id_sala, inicio=None, fin=None):
        # Base de la query
    query = """
        SELECT id_sala, toDate(fecha_evento) AS order_date_readable,
               tipo_sala, estado, responsable, descripcion
        FROM logs_by_room
        WHERE id_sala = ?
    """

    params = [id_sala]

    # Agrega filtros opcionales
    if inicio is not None:
        query += " AND fecha_evento >= minTimeuuid(?)"
        params.append(inicio)

    if fin is not None:
        query += " AND fecha_evento <= maxTimeuuid(?)"
        params.append(fin)
    
    # Preparar y ejecutar
    stmt = session.prepare(query)
    rows = session.execute(stmt, params)

    # Mostrar resultados
    print(f"\n=== Logs de la Sala: {id_sala} ===")
    if inicio or fin:
        print(f"Rango: {inicio} → {fin}\n")

    for row in rows:
        print(f"\nSala: {row.id_sala}")
        print(f"  - Fecha: {row.order_date_readable}")
        print(f"  - Tipo Sala: {row.tipo_sala}")
        print(f"  - Estado: {row.estado}")
        print(f"  - Responsable: {row.responsable}")
        print(f"  - Descripcion: {row.descripcion}")
        print()
