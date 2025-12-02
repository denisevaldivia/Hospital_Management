from datetime import datetime
import pprint

def query_1(db):
    print("--- Crear Registro de Paciente ---")
    nombre = input("Nombre completo: ")
    genero = input("Género: ")
    fecha_str = input("Fecha de nacimiento (YYYY-MM-DD): ")
    tel = input("Teléfono: ")
    email = input("Email: ")
    
    try:
        fecha_nac = datetime.strptime(fecha_str, "%Y-%m-%d")
        documento = {
            "nombre": nombre,
            "genero": genero,
            "fecha_nacimiento": fecha_nac,
            "contacto": {
                "telefono": tel,
                "email": email
            }
        }
        resultado = db.pacientes.insert_one(documento)
        print(f"Paciente insertado con ID: {resultado.inserted_id}")
    except ValueError:
        print("Formato de fecha incorrecto.")

def query_2(db):
    print("--- Crear Expediente de Doctor ---")
    nombre = input("Nombre del Doctor: ")
    especialidad = input("Especialidad: ")
    licencia = input("Licencia/Cédula: ")
    tel = input("Teléfono: ")
    email = input("Email: ")
    ext = input("Extensión: ")

    documento = {
        "nombre": nombre,
        "especialidad": especialidad,
        "licencia": licencia,
        "contacto": {
            "telefono": tel,
            "email": email,
            "extension": ext
        }
    }
    resultado = db.doctores.insert_one(documento)
    print(f"Doctor insertado con ID: {resultado.inserted_id}")

def query_3(db):
    print("--- Agregar Medicamento al Catálogo ---")
    nombre = input("Nombre del medicamento (con gramaje): ")
    try:
        stock = int(input("Stock inicial: "))
        documento = {
            "nombre": nombre,
            "stock": stock
        }
        resultado = db.medicamentos.insert_one(documento)
        print(f"Medicamento insertado con ID: {resultado.inserted_id}")
    except ValueError:
        print("El stock debe ser un número entero.")

def query_4(db):
    print("--- Crear Especialidad ---")
    nombre = input("Nombre de la Especialidad: ")
    try:
        piso = int(input("Piso: "))
        equipo_str = input("Equipo médico (separado por comas): ")
        descripcion = input("Descripción: ")
        
        equipo_lista = [e.strip() for e in equipo_str.split(',')]
        
        documento = {
            "especialidad": nombre,
            "piso": piso,
            "equipo": equipo_lista,
            "descripcion": descripcion
        }
        resultado = db.especialidades.insert_one(documento)
        print(f"Especialidad insertada con ID: {resultado.inserted_id}")
    except ValueError:
        print("El piso debe ser un número entero.")

def query_5(db):
    print("--- Crear Servicio Hospitalario ---")
    id_srv = input("ID del Servicio (ej. SRV-00X): ")
    nombre = input("Nombre del Servicio: ")
    descripcion = input("Descripción: ")

    documento = {
        "id_servicio": id_srv,
        "nombre": nombre,
        "descripcion": descripcion
    }
    resultado = db.servicios.insert_one(documento)
    print(f"Servicio insertado con ID: {resultado.inserted_id}")

def query_6(db):
    print("--- Total de Pacientes Registrados ---")
    pipeline = [
        { "$count": "total_pacientes" }
    ]
    resultado = list(db.pacientes.aggregate(pipeline))
    if resultado:
        print(f"Total: {resultado[0]['total_pacientes']}")
    else:
        print("Total: 0")

def query_7(db):
    print("--- Buscar Paciente Específico ---")
    nombre_busqueda = input("Ingrese el nombre del paciente a buscar: ")
    filtro = {"nombre": {"$regex": nombre_busqueda, "$options": "i"}}
    
    resultados = db.pacientes.find(filtro)
    encontrado = False
    for doc in resultados:
        encontrado = True
        pprint.pprint(doc)
    
    if not encontrado:
        print("No se encontraron pacientes con ese nombre.")

def query_8(db):
    print("--- Total de Doctores Registrados ---")
    pipeline = [
        { "$count": "total_doctores" }
    ]
    resultado = list(db.doctores.aggregate(pipeline))
    if resultado:
        print(f"Total: {resultado[0]['total_doctores']}")
    else:
        print("Total: 0")

def query_9(db):
    print("--- Buscar Doctor ---")
    opcion = input("Buscar por (1) Nombre o (2) Especialidad: ")
    filtro = {}
    
    if opcion == "1":
        val = input("Nombre del doctor: ")
        filtro = {"nombre": {"$regex": val, "$options": "i"}}
    elif opcion == "2":
        val = input("Especialidad: ")
        filtro = {"especialidad": {"$regex": val, "$options": "i"}}
    
    resultados = db.doctores.find(filtro)
    for doc in resultados:
        pprint.pprint(doc)

def query_10(db):
    print("--- Buscar Medicamento ---")
    nombre_med = input("Nombre del medicamento: ")
    filtro = {"nombre": {"$regex": nombre_med, "$options": "i"}}
    
    resultados = db.medicamentos.find(filtro)
    for doc in resultados:
        pprint.pprint(doc)

def query_11(db):
    print("--- Listado de Especialidades ---")
    resultados = db.especialidades.find({}, {"_id": 0, "especialidad": 1, "piso": 1, "descripcion": 1})
    for doc in resultados:
        print(f"Especialidad: {doc.get('especialidad')} | Piso: {doc.get('piso')}")
        print(f"Descripción: {doc.get('descripcion')}\n")

def query_12(db):
    print("--- Listado de Servicios ---")
    resultados = db.servicios.find({}, {"_id": 0, "nombre": 1, "descripcion": 1})
    for doc in resultados:
        print(f"Servicio: {doc.get('nombre')}")
        print(f"Descripción: {doc.get('descripcion')}\n")