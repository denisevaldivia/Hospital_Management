# ------------------------------------
#   Opcion 1 = POPULAR DGRAPH
# ------------------------------------

# populated.py
import pydgraph
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

def populate_data(client):
    # --- Crear doctores ---
    doctors = []
    especialidades = ["Cardiología", "Neurología", "Pediatría", "Oncología", "Dermatología"]
    for i in range(1, 6):
        especialidad = random.choice(especialidades)
        doc = {
            "uid": f"_:{i}",
            "dgraph.type": "Doctor",
            "id_doctor": i,
            "nombre": fake.name(),
            "especialidad": especialidad,
            "licencia": f"LIC{i:04d}",
            "anios_experiencia": random.randint(5, 30),
            "correo": fake.email(),
            "telefono": fake.random_number(digits=10),
            "AGENDA": [],
            "ATIENDEN": [],
            "OTORGAN": []
        }
        doctors.append(doc)

    # Separar doctores pediatría y adultos
    pediatras = [d for d in doctors if d["especialidad"] == "Pediatría"]
    adultos = [d for d in doctors if d["especialidad"] != "Pediatría"]

    # --- Crear pacientes ---
    patients = []
    for i in range(1, 11):
        sexo = random.choice(["M", "F"])

        # Decidir si paciente es pediátrico o adulto
        if random.random() < 0.3 and pediatras:  # 30% de pacientes pediátricos
            doctor = random.choice(pediatras)
            birth_date = fake.date_of_birth(minimum_age=0, maximum_age=17)
        else:
            doctor = random.choice(adultos)
            birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)

        pat = {
            "uid": f"_:{100+i}",
            "dgraph.type": "Paciente",
            "id_paciente": i,
            "nombre": fake.name(),
            "sexo": sexo,
            "fecha_nacimiento": birth_date.isoformat(),
            "edad": (datetime.now().date() - birth_date).days // 365,
            "ATIENDEN": [{"uid": doctor["uid"]}]
        }

        # Relación inversa en doctor
        doc_uid = pat["ATIENDEN"][0]["uid"]
        for doc in doctors:
            if doc["uid"] == doc_uid:
                doc["ATIENDEN"].append({"uid": pat["uid"]})
                break

        patients.append(pat)

    # --- Crear salas ---
    rooms = []
    for i in range(1, 4):
        doctor = random.choice(doctors)
        room = {
            "uid": f"_:{200+i}",
            "dgraph.type": "Sala",
            "id_sala": i,
            "tipo": random.choice(["Consulta", "Cirugía", "Urgencias", "Laboratorios"]),
            "id_doctor": doctor["id_doctor"]
        }
        doctor["AGENDA"].append({"uid": room["uid"]})
        rooms.append(room)

    # --- Crear servicios ---
    services = []
    for i in range(1, 6):
        patient = random.choice(patients)
        srv = {
            "uid": f"_:{300+i}",
            "dgraph.type": "Servicio",
            "nombre_servicio": random.choice(["Ultrasonido", "RayosX", "Análisis clínicos", "Anatomía patológica", "Electrocardigrama", "Análisis de orina", "Toma de glucosa en sangre"]),
            "precio": round(random.uniform(50, 9900), 2),
            "GENERAN": [{"uid": patient["uid"]}]
        }
        services.append(srv)

    # --- Crear transacciones ---
    transactions = []
    for i in range(1, 11):
        patient = random.choice(patients)
        tr = {
            "uid": f"_:{400+i}",
            "dgraph.type": "Transaccion",
            "id_transaccion": i,
            "nombre_servicio": random.choice(services)["nombre_servicio"],
            "precio": round(random.uniform(50, 9900), 2),
            "GENERAN": [{"uid": patient["uid"]}]
        }
        transactions.append(tr)

    # --- Crear recetas ---
    # Diccionario de enfermedad → medicamentos válidos
    enfermedad_medicamentos = {
        "Gripe": ["Paracetamol", "Ibuprofeno", "Oseltamivir", "Aspirina"],
        "Hipertensión": ["Losartán", "Enalapril", "Amlodipino"],
        "Diabetes": ["Metformina", "Insulina", "Glipizida"],
        "Asma": ["Salbutamol", "Budesonida", "Montelukast"],
        "Migraña": ["Sumatriptán", "Ibuprofeno", "Naproxeno"]
    }

    prescriptions = []
    for i in range(1, 11):
        patient = random.choice(patients)
        doctor = random.choice(doctors)
        
        # Elegir diagnóstico
        diagnostico = random.choice(list(enfermedad_medicamentos.keys()))
        
        # Elegir un medicamento válido para ese diagnóstico
        medicina = random.choice(enfermedad_medicamentos[diagnostico])
        
        pres = {
            "uid": f"_:{500+i}",
            "dgraph.type": "Receta",
            "id_receta": i,
            "id_paciente": patient["id_paciente"],
            "id_doctor": doctor["id_doctor"],
            "diagnostico": diagnostico,
            "fecha_emision": datetime.now().isoformat(),
            "medicina": medicina,
            "cantidad": random.randint(1, 10),
            "frecuencia": random.randint(1, 3),
            "TIENEN": [{"uid": patient["uid"]}],
            "OTORGAN": [{"uid": doctor["uid"]}]
        }
        prescriptions.append(pres)


    # --- Crear visitas ---
    visits = []
    for i in range(1, 6):
        patient = random.choice(patients)
        visit = {
            "uid": f"_:{600+i}",
            "dgraph.type": "Visita",
            "id_paciente": patient["id_paciente"],
            "nombre_visitante": fake.name(),
            "relacion_paciente": random.choice(["Padre", "Madre", "Hermano", "Amig@", "Novi@", "Hermana", "Tia", "Sobrino"]),
            "motivo": random.choice(["Acompañamiento", "Revisión", "Emergencia", "Platicar"]),
            "hora_entrada": fake.date_time_this_month().isoformat(),
            "hora_salida": (datetime.now() + timedelta(hours=random.randint(1,3))).isoformat(),
            "RECIBEN": [{"uid": patient["uid"]}]
        }
        visits.append(visit)

    # --- Combinar todos los nodos ---
    all_nodes = doctors + patients + rooms + services + transactions + prescriptions + visits

    # --- Insertar en Dgraph ---
    txn = client.txn()
    try:
        txn.mutate(set_obj=all_nodes)
        txn.commit()
        print(" Datos creados con éxito en Dgraph!")
    finally:
        txn.discard()
