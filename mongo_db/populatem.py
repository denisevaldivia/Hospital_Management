
# ------------------------------------
#   Opcion 1 = POPULAR MONGODB
# ------------------------------------

from datetime import datetime

def populate_data(db):

    print("\n--- POPULATING DATABASE MONGO ---\n")

    # Limpiar colecciones antes de insertar
    db.pacientes.delete_many({})
    db.doctores.delete_many({})
    db.medicamentos.delete_many({})
    db.especialidades.delete_many({})
    db.servicios.delete_many({})

    # ---------- PACIENTES ----------
    pacientes = [
        {
            "nombre": "Juan Pérez García",
            "genero": "Masculino",
            "fecha_nacimiento": datetime(1985, 3, 15),
            "contacto": {"telefono": "3312345678", "email": "juan.perez@email.com"}
        },
        {
            "nombre": "María López Hernández",
            "genero": "Femenino",
            "fecha_nacimiento": datetime(1990, 7, 22),
            "contacto": {"telefono": "3319087765", "email": "maria.lopez@email.com"}
        },
        {
            "nombre": "Carlos Ramírez Soto",
            "genero": "Masculino",
            "fecha_nacimiento": datetime(1978, 1, 10),
            "contacto": {"telefono": "3322213344", "email": "carlos.ramirez@email.com"}
        },
        {
            "nombre": "Luisa Fernández Torres",
            "genero": "Femenino",
            "fecha_nacimiento": datetime(2000, 5, 5),
            "contacto": {"telefono": "3345678901", "email": "luisa.fernandez@email.com"}
        },
        {
            "nombre": "Miguel Ángel Castillo",
            "genero": "Masculino",
            "fecha_nacimiento": datetime(1995, 9, 30),
            "contacto": {"telefono": "3311223344", "email": "miguel.castillo@email.com"}
        }
    ]

    # ---------- DOCTORES ----------
    doctores = [
        {
            "nombre": "Dra. Ana Martínez López",
            "especialidad": "Cardiología",
            "licencia": "12345678",
            "contacto": {"telefono": "3323456789", "email": "ana.martinez@hospital.com", "extension": "1234"}
        },
        {
            "nombre": "Dr. Pedro González Ruiz",
            "especialidad": "Neurología",
            "licencia": "87654321",
            "contacto": {"telefono": "3328890011", "email": "pedro.gonzalez@hospital.com", "extension": "1321"}
        },
        {
            "nombre": "Dra. Sofía Méndez Aguilar",
            "especialidad": "Pediatría",
            "licencia": "44556677",
            "contacto": {"telefono": "3330124587", "email": "sofia.mendez@hospital.com", "extension": "1402"}
        },
        {
            "nombre": "Dr. Alberto Torres Durán",
            "especialidad": "Ortopedia",
            "licencia": "99887766",
            "contacto": {"telefono": "3335678923", "email": "alberto.torres@hospital.com", "extension": "1503"}
        },
        {
            "nombre": "Dra. Elena Rivas Montoya",
            "especialidad": "Ginecología",
            "licencia": "11223344",
            "contacto": {"telefono": "3321239876", "email": "elena.rivas@hospital.com", "extension": "1620"}
        }
    ]

    # ---------- MEDICAMENTOS ----------
    medicamentos = [
        {"nombre": "Paracetamol 500mg", "stock": 5000},
        {"nombre": "Ibuprofeno 400mg", "stock": 2000},
        {"nombre": "Omeprazol 20mg", "stock": 1500},
        {"nombre": "Amoxicilina 500mg", "stock": 1200},
        {"nombre": "Metformina 850mg", "stock": 1800},
    ]

    # ---------- ESPECIALIDADES ----------
    especialidades = [
        {
            "especialidad": "Cardiología",
            "piso": 3,
            "equipo": ["Electrocardiógrafos", "Ecocardiógrafo", "Holter", "Monitores cardiacos"],
            "descripcion": "Estudia, diagnostica y trata enfermedades del corazón."
        },
        {
            "especialidad": "Neurología",
            "piso": 4,
            "equipo": ["Electroencefalógrafos", "TAC cerebral", "Equipo de punción lumbar"],
            "descripcion": "Atención y diagnóstico del sistema nervioso."
        },
        {
            "especialidad": "Pediatría",
            "piso": 2,
            "equipo": ["Incubadoras", "Cunas térmicas", "Monitores neonatales"],
            "descripcion": "Atención médica para niños y adolescentes."
        },
        {
            "especialidad": "Ortopedia",
            "piso": 5,
            "equipo": ["Rayos X", "Equipo de yesos", "Mesas ortopédicas"],
            "descripcion": "Diagnóstico y tratamiento de lesiones musculoesqueléticas."
        },
        {
            "especialidad": "Ginecología",
            "piso": 3,
            "equipo": ["Ultrasonido", "Colposcopio", "Monitores fetales"],
            "descripcion": "Atención de salud reproductiva y femenina."
        }
    ]

    # ---------- SERVICIOS ----------
    servicios = [
        {
            "id_servicio": "SRV-001",
            "nombre": "Rayos X",
            "descripcion": "Servicio de radiografías digitales para diagnóstico."
        },
        {
            "id_servicio": "SRV-002",
            "nombre": "Laboratorio Clínico",
            "descripcion": "Pruebas sanguíneas, químicas y microbiológicas."
        },
        {
            "id_servicio": "SRV-003",
            "nombre": "Ultrasonido",
            "descripcion": "Imágenes diagnósticas mediante ultrasonido."
        },
        {
            "id_servicio": "SRV-004",
            "nombre": "Tomografía",
            "descripcion": "Estudios avanzados mediante tomografía computarizada."
        },
        {
            "id_servicio": "SRV-005",
            "nombre": "Urgencias",
            "descripcion": "Atención inmediata para emergencias y traumas."
        }
    ]

    try:
        db.pacientes.insert_many(pacientes)
        db.doctores.insert_many(doctores)
        db.medicamentos.insert_many(medicamentos)
        db.especialidades.insert_many(especialidades)
        db.servicios.insert_many(servicios)

        print("Datos insertados exitosamente en todas las colecciones de Mongo.\n")

    except Exception as e:
        print("Error al insertar datos:", e)
