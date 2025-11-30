
# ------------------------------------
#   Opcion 1 = POPULAR CASSANDRA
# ------------------------------------

from cassandra.query import SimpleStatement
import uuid
import random
from datetime import datetime


def bulk_insert(session):

    print("\n--- POPULATING DATABASE CASSANDRA ---\n")

    # --- Datos de ejemplo ---
    pacientes = [f"PACIENTE_{i}" for i in range(1, 6)]
    donadores = [f"DONADOR_{i}" for i in range(1, 6)]
    visitantes = [f"VISITANTE_{i}" for i in range(1, 6)]
    cuentas = [f"CUENTA_{i}" for i in range(1, 6)]
    salas = [f"SALA_{i}" for i in range(1, 6)]
    doctores = ["Dra. Gómez", "Dr. López", "Dr. Ramírez", "Dr. Pérez", "Dra. Fernández"]
    estados_sala = ["Libre", "Ocupada", "En limpieza", "Reservada"]
    medicamentos = ["Metformina", "Amoxicilina", "Omeprazol", "Ibuprofeno", "Paracetamol"]
    observaciones = ["Tomar cada 8hrs", "Tomar cada 4hrs", "Tomar cada 12hrs", "Tomar cada 24hrs", "Una sola toma", "Regresar en 5 días para seguimiento"]
    vacunas = ["Hepatitis B", "Influenza", "Tétanos", "SRP", "COVID-19"]
    tipo_vacuna = ["Dosis única", "Series múltiples dosis", "Dosis de refuerzo periódicas"]
    tipos_sangre = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
    tipos_pago = ["Efectivo", "Tarjeta", "Transferencia"]
    tipos_transaccion = [1,2,3,4,5]
    motivo = ["Familiar", "Emergencia", "Acompañante", "Platicar"]
    tipo_sala = ["Consulta", "Laboratorio", "Terapia"]
    relacion_paciente = ["Padre","Madre","Hijo","Amig@", "Pareja"]
    nombre_visitante = ["Joel", "Maria", "Jonas", "Lulu", "David", "Jen"]
    nombre = ["Rosario", "Dexter", "Jane", "Jorge", "Luz", "Henry"]


    # --- Prescriptions ---
    for paciente in pacientes:
        session.execute("""
            INSERT INTO prescriptions_by_patient
            (exp_paciente, fecha_creacion, doctor_responsable, medicamentos, observaciones)
            VALUES (%s, %s, %s, %s, %s)
        """, (paciente, uuid.uuid1(), random.choice(doctores), random.choice(medicamentos), random.choice(observaciones)))

    # --- Historial médico ---
    for paciente in pacientes:
        session.execute("""
            INSERT INTO historial_by_patient
            (exp_paciente, fecha_diagnostico, descripcion, observaciones, doctor_responsable)
            VALUES (%s, %s, %s, %s, %s)
        """, (paciente,
                uuid.uuid1(), 
                random.choice(["Paciente consciente y orientado", "Signos vitales dentro de parámetros normales", "El paciente refiere malestar leve, sin dificultad respiratoria evidente"]),
                random.choice(["Se identifica una molestia leve en el ojo izquierdo fuera del diagnostico inicial", "Se identifica un proceso sin mayor complicación", "Se requiere una cita de seguimiento"]), 
                random.choice(doctores)))

    # --- Blood donation ---
    for donador in donadores:
        session.execute("""
            INSERT INTO blood_donation_by_patient
            (exp_donador, fecha_donacion, tipo_sangre, nombre, cantidad)
            VALUES (%s, %s, %s, %s, %s)
        """, (donador, uuid.uuid1(), random.choice(tipos_sangre), random.choice(nombre), random.uniform(1, 2)))

    # --- Vaccines ---
    for paciente in pacientes:
        session.execute("""
            INSERT INTO vaccines_by_patient
            (exp_paciente, fecha_vacuna, tipo_vacuna, dosis, lote)
            VALUES (%s, %s, %s, %s, %s)
        """, (paciente, uuid.uuid1(), random.choice(vacunas), random.choice(tipo_vacuna), str(uuid.uuid4())))

    # --- Visits ---
    for visitante in visitantes:
        session.execute("""
            INSERT INTO logs_by_visit
            (expediente, fecha_visita, nombre_visitante, motivo, duracion, relacion_paciente)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (visitante, uuid.uuid1(), random.choice(nombre_visitante), random.choice(motivo), random.randint(10,120), random.choice(relacion_paciente)))

    # --- Transactions ---
    for cuenta in cuentas:
        session.execute("""
            INSERT INTO logs_by_transactions
            (cuenta, fecha_pago, tipo_transaccion, monto, metodo_pago, folio)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (cuenta, uuid.uuid1(), random.choice(tipos_transaccion), round(random.uniform(100,5000),2), random.choice(tipos_pago), uuid.uuid4()))

    # --- Room logs ---
    for sala in salas:
        session.execute("""
            INSERT INTO logs_by_room
            (id_sala, fecha_evento, tipo_sala, estado, responsable, descripcion)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            sala, 
            uuid.uuid1(), 
            random.choice(tipo_sala), 
            random.choice(estados_sala), 
            random.choice(doctores), 
            random.choice(["Sala privada y tranquila, diseñada para ofrecer comodidad al paciente.", "Los colores de la sala son claros para mantener la sensación de limpieza.", "Sala bien equipada para toda clase de procedimientos"])
        ))



    print("Datos insertados a cada table exitosamente en Cassandra.")
