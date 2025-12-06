import csv
import pandas as pd

def populate_data(client):

    # ------------------ CARGA DE NODOS ------------------ #
    def load_doctores(file_path):
        txn = client.txn()
        resp = None
        try:
            doctores = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    doctores.append({
                        'uid': '_:' + row['id_doctor'],
                        'id_doctor': int(row['id_doctor']),
                        'nombre': row['nombre'],
                        'especialidad': row['especialidad'],
                        'licencia': row['licencia'],
                        'anios_experiencia': int(row['anios_experiencia']),
                        'correo': row['correo'],
                        'telefono': row['telefono'],
                        'dgraph.type': 'Doctor'
                    })
            resp = txn.mutate(set_obj=doctores)
            txn.commit()
        finally:
            txn.discard()
        return resp.uids

    def load_pacientes(file_path):
        txn = client.txn()
        resp = None
        try:
            pacientes = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pacientes.append({
                        'uid': '_:' + row['id_paciente'],
                        'id_paciente': int(row['id_paciente']),
                        'nombre': row['nombre'],
                        'sexo': row['sexo'],
                        'edad': int(row['edad']),
                        'fecha_nacimiento': pd.to_datetime(row['fecha_nacimiento']).strftime("%Y-%m-%dT%H:%M:%S"),
                        'dgraph.type': 'Paciente'
                    })
            resp = txn.mutate(set_obj=pacientes)
            txn.commit()
        finally:
            txn.discard()
        return resp.uids
    
    def load_servicios(file_path):
        txn = client.txn()
        resp = None
        try:
            servicios = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    servicios.append({
                        'uid': '_:' + row['id_servicio'],
                        'id_servicio': int(row['id_servicio']),
                        'nombre_servicio': row['nombre_servicio'],
                        'dgraph.type': 'Servicio'
                    })
            resp = txn.mutate(set_obj=servicios)
            txn.commit()
        finally:
            txn.discard()
        return resp.uids
    
    def load_transacciones(file_path):
        txn = client.txn()
        resp = None
        try:
            transacciones = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    transacciones.append({
                        'uid': '_:' + row['id_transaccion'],
                        'id_transaccion': int(row['id_transaccion']),
                        'precio': float(row['precio']),
                        'dgraph.type': 'Transaccion'
                    })
            resp = txn.mutate(set_obj=transacciones)
            txn.commit()
        finally:
            txn.discard()
        return resp.uids
    
    def load_recetas(file_path):
        txn = client.txn()
        resp = None
        try:
            recetas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    recetas.append({
                        'uid': '_:' + row['id_receta'],
                        'id_receta': int(row['id_receta']),
                        'diagnostico': row['diagnostico'],
                        'medicina': row['medicina'],
                        'cantidad': int(row['cantidad']),
                        'frecuencia': int(row['frecuencia']),
                        'fecha_emision': pd.to_datetime(row['fecha_emision']).strftime("%Y-%m-%dT%H:%M:%S"),
                        'dgraph.type': 'Receta'
                    })
            resp = txn.mutate(set_obj=recetas)
            txn.commit()
        finally:
            txn.discard()
        return resp.uids

    def load_visitas(file_path):
        txn = client.txn()
        resp = None
        try:
            visitas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    visitas.append({
                        'uid': '_:' + row['id_visita'],
                        'id_visita': int(row['id_visita']),
                        'nombre_visitante': row['nombre_visitante'],
                        'relacion_paciente': row['relacion_paciente'],
                        'motivo': row['motivo'],
                        'hora_entrada': pd.to_datetime(row['hora_entrada']).strftime("%Y-%m-%dT%H:%M:%S"),
                        'hora_salida': pd.to_datetime(row['hora_salida']).strftime("%Y-%m-%dT%H:%M:%S"),
                        'dgraph.type': 'Visita'
                    })
            resp = txn.mutate(set_obj=visitas)
            txn.commit()
        finally:
            txn.discard()
        return resp.uids
    
    def load_salas(file_path):
        txn = client.txn()
        resp = None
        try:
            salas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    salas.append({
                        'uid': '_:' + row['id_sala'],
                        'id_sala': int(row['id_sala']),
                        'nombre_sala': row['nombre_sala'],
                        'tipo': row['tipo'],
                        'dgraph.type': 'Sala'
                    })
            resp = txn.mutate(set_obj=salas)
            txn.commit()
        finally:
            txn.discard()
        return resp.uids

    # ------------------ RELACIONES ------------------ #
    def create_atiende_edges(file_path, doctores_uid, pacientes_uid):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    doctor_id = row['id_doctor']
                    paciente_id = row['id_paciente']

                    mutation = {
                        'uid': doctores_uid[doctor_id],
                        'ATIENDE': {
                            'uid': pacientes_uid[paciente_id]
                        }
                    }

                    print(f"{doctor_id} (Doctor) -ATIENDE-> {paciente_id} (Paciente)")
                    txn.mutate(set_obj=mutation)
            txn.commit()
        finally:
            txn.discard()

    def create_otorga_edges(file_path, doctores_uid, recetas_uid):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mutation = {
                        'uid': doctores_uid[row['id_doctor']],
                        'OTORGA': {'uid': recetas_uid[row['id_receta']]}
                    }
                    print(f"{row['id_doctor']} (Doctor) -OTORGA-> {row['id_receta']} (Receta)")
                    txn.mutate(set_obj=mutation)
            txn.commit()
        finally:
            txn.discard()
    
    def create_tiene_edges(file_path, pacientes_uid, recetas_uid):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mutation = {
                        'uid': pacientes_uid[row['id_paciente']],
                        'TIENE': {'uid': recetas_uid[row['id_receta']]}
                    }
                    print(f"{row['id_paciente']} (Paciente) -TIENE-> {row['id_receta']} (Receta)")
                    txn.mutate(set_obj=mutation)
            txn.commit()
        finally:
            txn.discard()
    
    def create_recibe_edges(file_path, pacientes_uid, visitas_uid):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mutation = {
                        'uid': pacientes_uid[row['id_paciente']],
                        'RECIBE': {'uid': visitas_uid[row['id_visita']]}
                    }
                    print(f"{row['id_paciente']} (Paciente) -RECIBE-> {row['id_visita']} (Visita)")
                    txn.mutate(set_obj=mutation)
            txn.commit()
        finally:
            txn.discard()

    def create_solicita_edges(file_path, pacientes_uid, servicios_uid):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mutation = {
                        'uid': pacientes_uid[row['id_paciente']],
                        'SOLICITA': {'uid': servicios_uid[row['id_servicio']]}
                    }
                    print(f"{row['id_paciente']} (Paciente) -SOLICITA-> {row['id_servicio']} (Servicio)")
                    txn.mutate(set_obj=mutation)
            txn.commit()
        finally:
            txn.discard()
    
    def create_genera_edges(file_path, servicios_uid, transacciones_uid):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mutation = {
                        'uid': servicios_uid[row['id_servicio']],
                        'GENERA': {'uid': transacciones_uid[row['id_transaccion']]}
                    }
                    print(f"{row['id_servicio']} (Servicio) -GENERA-> {row['id_transaccion']} (Transaccion)")
                    txn.mutate(set_obj=mutation)
            txn.commit()
        finally:
            txn.discard()
    
    def create_agenda_edges(file_path, doctores_uid, salas_uid):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mutation = {
                        'uid': doctores_uid[row['id_doctor']],
                        'AGENDA': {'uid': salas_uid[row['id_sala']]}
                    }
                    print(f"{row['id_doctor']} (Doctor) -AGENDA-> {row['id_sala']} (Sala)")
                    txn.mutate(set_obj=mutation)
            txn.commit()
        finally:
            txn.discard()

    # ------------------ EJECUCIÓN ------------------ #
    doctores_uid = load_doctores('dgraph_db/csv_data/nodos/doctores.csv')
    pacientes_uid = load_pacientes('dgraph_db/csv_data/nodos/pacientes.csv')
    servicios_uid = load_servicios('dgraph_db/csv_data/nodos/servicios.csv')
    transacciones_uid = load_transacciones('dgraph_db/csv_data/nodos/transacciones.csv')
    recetas_uid = load_recetas('dgraph_db/csv_data/nodos/recetas.csv')
    visitas_uid = load_visitas('dgraph_db/csv_data/nodos/visitas.csv')
    salas_uid = load_salas('dgraph_db/csv_data/nodos/salas.csv')

    create_atiende_edges('dgraph_db/csv_data/relaciones/atiende_doctor_paciente.csv', doctores_uid, pacientes_uid)
    create_otorga_edges('dgraph_db/csv_data/relaciones/otorga_doctor_receta.csv', doctores_uid, recetas_uid)
    create_tiene_edges('dgraph_db/csv_data/relaciones/tiene_paciente_receta.csv', pacientes_uid, recetas_uid)
    create_recibe_edges('dgraph_db/csv_data/relaciones/recibe_paciente_visita.csv', pacientes_uid, visitas_uid)
    create_solicita_edges('dgraph_db/csv_data/relaciones/solicita_paciente_servicio.csv', pacientes_uid, servicios_uid)
    create_genera_edges('dgraph_db/csv_data/relaciones/genera_servicio_transaccion.csv', servicios_uid, transacciones_uid)
    create_agenda_edges('dgraph_db/csv_data/relaciones/agenda_doctor_sala.csv', doctores_uid, salas_uid)

    print("Datos cargados correctamente.")