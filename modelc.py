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

def query_1(session): pass
def query_2(session): pass
def query_3(session): pass
def query_4(session): pass
def query_5(session): pass
def query_7(session): pass
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
        print(f"Paciente: {row.exp_paciente}")
        print(f"  - Fecha: {row.order_date_readable}")
        print(f"  - Doctor Responsable: {row.doctor_responsable}")
        print(f"  - Medicamentos: {row.medicamentos}")
        print(f"  - Observaciones: {row.observaciones}")
        print()

def query_9(session): pass
def query_10(session): pass
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

        print(f"Paciente: {row.exp_paciente}")
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
        print(f"Paciente: {row.exp_paciente}")
        print(f"  - Fecha: {row.order_date_readable}")
        print(f"  - Nombre del Visitante: {row.nombre_visitante}")
        print(f"  - Relación: {row.relacion_paciente}")
        print(f"  - Motivo: {row.motivo}")
        print(f"  - Duracion: {row.duracion}")
        print()

def query_13(session): pass
def query_14(session): pass
