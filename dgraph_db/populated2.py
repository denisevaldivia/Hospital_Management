import datetime
import json
import csv
import pydgraph
import pandas as pd
import os

def populate_data(client):
    # Funciones internas para cargar nodos
    def load_servicios(file_path):
        txn = client.txn()
        resp = None
        try:
            servicios = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    servicios.append({
                        'uid': '_:' + row['id_paciente'],
                        'id_paciente': int(row['id_paciente']),
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
                        'nombre_servicio': row['nombre_servicio'],
                        'precio': float(row['precio']),
                        'dgraph.type': 'Transaccion'
                    })
                resp = txn.mutate(set_obj=transacciones)
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
                        'fecha_nacimiento': pd.to_datetime(row['fecha_nacimiento']).isoformat(),
                        'dgraph.type': 'Paciente'
                    })
                resp = txn.mutate(set_obj=pacientes)
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
                        'id_doctor': int(row['id_doctor']),
                        'tipo': row['tipo'],
                        'dgraph.type': 'Sala'
                    })
                resp = txn.mutate(set_obj=salas)
            txn.commit()
        finally:
            txn.discard()

        return resp.uids
    
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
                        'telefono': int(row['telefono']),
                        'dgraph.type': 'Doctor'
                    })
                resp = txn.mutate(set_obj=doctores)
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
                        'uid': '_:' + row['id_paciente'],
                        'id_paciente': int(row['id_paciente']),
                        'nombre_visitante': row['nombre_visitante'],
                        'relacion_paciente': row['relacion_paciente'],
                        'motivo': row['motivo'],
                        'hora_entrada': pd.to_datetime(row['hora_entrada']).isoformat(),
                        'hora_salida': pd.to_datetime(row['hora_salida']).isoformat(),
                        'dgraph.type': 'Visita'
                    })
                resp = txn.mutate(set_obj=visitas)
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
                        'id_paciente': int(row['id_paciente']),
                        'id_doctor': int(row['id_doctor']),
                        'diagnostico': row['diagnostico'],
                        'fecha_emision': pd.to_datetime(row['fecha_emision']).isoformat(),
                        'medicina': row['medicina'],
                        'cantidad': int(row['cantidad']),
                        'frecuencia': int(row['frecuencia']),
                        'dgraph.type': 'Receta'
                    })
                resp = txn.mutate(set_obj=recetas)
            txn.commit()
        finally:
            txn.discard()

        return resp.uids

    # Crear relaciones usando uid
    def create_edge(client, file_path, from_uids, to_uids, predicado, col_from, col_to):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    f_uid = from_uids[row[col_from]]   # origen
                    t_uid = to_uids[row[col_to]]       # destino

                    mutation = {
                        'uid': f_uid,
                        predicado: { 'uid': t_uid }
                    }

                    print(f"Creando relacion {f_uid} -{predicado}-> {t_uid}")
                    txn.mutate(set_obj=mutation)

            txn.commit()
        finally:
            txn.discard()


    # Establecer directorio
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))       # /dgraph_db
    NODES_DIR = os.path.join(ROOT_DIR, 'csv_data/nodos')        # /dgraph_db/csv_data/nodos
    EDGES_DIR = os.path.join(ROOT_DIR, 'csv_data/relaciones')   # /dgraph_db/csv_data/relaciones

    # Ejecutar carga de datos
    pacientes_uid =load_pacientes(os.path.join(NODES_DIR, 'pacientes.csv'))
    doctores_uid =load_doctores(os.path.join(NODES_DIR, 'doctores.csv'))
    salas_uid =load_salas(os.path.join(NODES_DIR, 'salas.csv'))
    servicios_uid =load_servicios(os.path.join(NODES_DIR, 'servicios.csv'))
    transacciones_uid =load_transacciones(os.path.join(NODES_DIR, 'transacciones.csv'))
    visitas_uid =load_visitas(os.path.join(NODES_DIR, 'visitas.csv'))
    recetas_uid =load_recetas(os.path.join(NODES_DIR, 'recetas.csv'))

    create_edge(client, os.path.join(EDGES_DIR, 'agenda_doctor_sala.csv'), doctores_uid, salas_uid, "AGENDA", "id_doctor", "id_sala")
    create_edge(client, os.path.join(EDGES_DIR, 'atiende_doctor_paciente.csv'), doctores_uid, pacientes_uid, "ATIENDE", "id_doctor", "id_paciente")
    create_edge(client, os.path.join(EDGES_DIR, 'genera_servicio_transaccion.csv'), servicios_uid, transacciones_uid, "GENERA", "id_paciente", "id_transaccion")
    create_edge(client, os.path.join(EDGES_DIR, 'otorga_doctor_receta.csv'), doctores_uid, recetas_uid, "OTORGA", "id_doctor", "id_receta")
    create_edge(client, os.path.join(EDGES_DIR, 'recibe_paciente_visita.csv'), pacientes_uid, visitas_uid, "RECIBE", "id_paciente", "id_paciente")
    create_edge(client, os.path.join(EDGES_DIR, 'solicita_paciente_servicio.csv'), pacientes_uid, servicios_uid, "SOLICITA", "id_paciente", "id_paciente")
    create_edge(client, os.path.join(EDGES_DIR, 'tiene_paciente_receta.csv'), pacientes_uid, recetas_uid, "TIENE", "id_paciente", "id_receta")

    print("Datos cargados correctamente.")