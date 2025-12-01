
# ------------------------------------
#   Opcion 1 = POPULAR CASSANDRA
# ------------------------------------

from cassandra.query import SimpleStatement
import uuid
import random
from datetime import datetime
from cassandra.util import uuid_from_time

FECHAS = [
    datetime(2024, 7, 20),
    datetime(2024, 10, 5),
    datetime(2025, 1, 18),
    datetime(2025, 5, 9),
    datetime(2025, 9, 25),
]


def pick_timeuuids(n):
    fechas = random.sample(FECHAS, n)
    return [uuid_from_time(f) for f in fechas]


def bulk_insert(session):

    print("\n--- POPULATING DATABASE CASSANDRA ---\n")

    # Datos de los IDs
    pacientes = [f"PA-{i:02d}" for i in range(1, 4)]
    donadores = [f"DO-{i:02d}" for i in range(1, 4)]
    visitantes = [f"VI-{i:02d}" for i in range(1, 4)]
    cuentas = [f"CT-{i:02d}" for i in range(1, 4)]
    salas = [f"SA-{i:02d}" for i in range(1, 4)]

    # Resto
    doctores = ["Dra. Gómez", "Dr. López", "Dr. Ramírez", "Dr. Pérez", "Dra. Fernández"]
    estados_sala = ["Libre", "Ocupada", "En limpieza", "Reservada"]
    medicamentos = ["Metformina", "Amoxicilina", "Omeprazol", "Ibuprofeno", "Paracetamol"]
    observaciones = ["Tomar cada 8hrs", "Tomar cada 4hrs", "Tomar cada 12hrs", "Tomar cada 24hrs", "Una sola toma", "Regresar en 5 días para seguimiento"]
    vacunas = ["Hepatitis B", "Influenza", "Tétanos", "SRP", "COVID-19"]
    tipo_vacuna = ["Dosis única", "Series múltiples dosis", "Dosis de refuerzo periódicas"]
    tipos_sangre = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
    tipos_pago = ["Efectivo", "Tarjeta", "Transferencia"]
    tipos_transaccion = ["Pago", "Reembolso", "Contracargo", "Cancelación"]
    motivo = ["Familiar", "Emergencia", "Acompañante", "Platicar"]
    tipo_sala = ["Consulta", "Laboratorio", "Terapia"]
    relacion_paciente = ["Padre","Madre","Hijo","Amig@", "Pareja"]
    nombre_visitante = ["Joel", "Maria", "Jonas", "Lulu", "David", "Jen"]
    nombre = ["Rosario", "Dexter", "Jane", "Jorge", "Luz", "Henry"]


    # --- Prescriptions ---
    for paciente in pacientes:
        for fecha in pick_timeuuids(2):
            session.execute("""
                INSERT INTO prescriptions_by_patient
                (exp_paciente, fecha_creacion, doctor_responsable, medicamentos, observaciones)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                paciente,
                fecha,
                random.choice(doctores),
                random.choice(medicamentos),
                random.choice(observaciones)
            ))

    # --- Historial médico ---
    for paciente in pacientes:
        for fecha in pick_timeuuids(2):
            session.execute("""
                INSERT INTO historial_by_patient
                (exp_paciente, fecha_diagnostico, descripcion, observaciones, doctor_responsable)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                paciente,
                fecha,
                random.choice([
                    "Paciente consciente y orientado",
                    "Signos vitales dentro de parámetros normales",
                    "El paciente refiere malestar leve"
                ]),
                random.choice([
                    "Revisión sin complicaciones",
                    "Se requiere cita de seguimiento",
                    "Evolución favorable"
                ]),
                random.choice(doctores)
            ))

    # --- Blood donation ---
    for donador in donadores:
        for fecha in pick_timeuuids(2):
            session.execute("""
                INSERT INTO blood_donation_by_patient
                (exp_donador, fecha_donacion, tipo_sangre, nombre, cantidad)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                donador,
                fecha,
                random.choice(tipos_sangre),
                random.choice(nombre),
                random.uniform(1, 2)
            ))

    # --- Vaccines ---
    for paciente in pacientes:
        for fecha in pick_timeuuids(2):
            session.execute("""
                INSERT INTO vaccines_by_patient
                (exp_paciente, fecha_vacuna, tipo_vacuna, dosis, lote)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                paciente,
                fecha,
                random.choice(vacunas),
                random.choice(tipo_vacuna),
                "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=5))
            ))

    # --- Visits ---
    for paciente in pacientes:
        for fecha in pick_timeuuids(2):
           session.execute("""
                INSERT INTO logs_by_visit
                (exp_paciente, fecha_visita, nombre_visitante, motivo, duracion, relacion_paciente)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (paciente, fecha, random.choice(nombre_visitante), random.choice(motivo), random.randint(10, 120), random.choice(relacion_paciente)))


    # --- Transactions ---
    for cuenta in cuentas:
        for fecha in pick_timeuuids(2):
            session.execute("""
                INSERT INTO logs_by_transactions
                (cuenta, fecha_pago, tipo_transaccion, monto, metodo_pago, folio)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                cuenta,
                fecha,
                random.choice(tipos_transaccion),
                round(random.uniform(100, 5000), 2),
                random.choice(tipos_pago),
                uuid.uuid4()
            ))

    # --- Room logs ---
    for sala in salas:
        for fecha in pick_timeuuids(2):
            session.execute("""
                INSERT INTO logs_by_room
                (id_sala, fecha_evento, tipo_sala, estado, responsable, descripcion)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                sala,
                fecha,
                random.choice(tipo_sala),
                random.choice(estados_sala),
                random.choice(doctores),
                random.choice([
                    "Sala equipada para procedimientos",
                    "Revisión de limpieza completada",
                    "Equipamiento verificado"
                ])
            ))

    print("Datos insertados exitosamente en Cassandra.")
