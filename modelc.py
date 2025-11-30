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
            PRIMARY KEY (exp_paciente, fecha_creacion)
        ) WITH CLUSTERING ORDER BY (fecha_creacion DESC);
    """)

    # Logs by transactions
    session.execute("""
        CREATE TABLE IF NOT EXISTS logs_by_transactions (
            cuenta text,
            fecha_pago timeuuid,
            tipo_transaccion float,
            monto float,
            metodo_pago text,
            folio uuid,
            PRIMARY KEY (cuenta, fecha_pago)
        ) WITH CLUSTERING ORDER BY (fecha_pago DESC);
    """)

    # Blood donation by patient
    session.execute("""
        CREATE TABLE IF NOT EXISTS blood_donation_by_patient (
            exp_donador text,
            fecha_donacion timeuuid,
            tipo_sangre text,
            nombre text,
            cantidad float,
            PRIMARY KEY (exp_donador, fecha_donacion)
        ) WITH CLUSTERING ORDER BY (fecha_donacion DESC);
    """)

    # Vaccines by patient
    session.execute("""
        CREATE TABLE IF NOT EXISTS vaccines_by_patient (
            exp_paciente text,
            fecha_vacuna timeuuid,
            tipo_vacuna text,
            dosis text,
            lote text,
            PRIMARY KEY (exp_paciente, fecha_vacuna)
        ) WITH CLUSTERING ORDER BY (fecha_vacuna DESC);
    """)

    # Logs by visit
    session.execute("""
        CREATE TABLE IF NOT EXISTS logs_by_visit (
            expediente text,
            fecha_visita timeuuid,
            nombre_visitante text,
            motivo text,
            duracion int,
            relacion_paciente text,
            PRIMARY KEY (expediente, fecha_visita)
        ) WITH CLUSTERING ORDER BY (fecha_visita DESC);
    """)

    # Historial by patient
    session.execute("""
        CREATE TABLE IF NOT EXISTS historial_by_patient (
            exp_paciente text,
            fecha_diagnostico timeuuid,
            descripcion text,
            observaciones text,
            doctor_responsable text,
            PRIMARY KEY (exp_paciente, fecha_diagnostico)
        ) WITH CLUSTERING ORDER BY (fecha_diagnostico DESC);
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
            PRIMARY KEY (id_sala, fecha_evento)
        ) WITH CLUSTERING ORDER BY (fecha_evento DESC);
    """)

    print("Schema creado con éxito en Cassandra")



# ------------------------------------
#   QUERIES VACÍAS
# ------------------------------------

def query_1(session): pass
def query_2(session): pass
def query_3(session): pass
def query_4(session): pass
def query_5(session): pass
def query_7(session): pass
def query_8(session): pass
def query_9(session): pass
def query_10(session): pass
def query_11(session): pass
def query_12(session): pass
def query_13(session): pass
def query_14(session): pass
