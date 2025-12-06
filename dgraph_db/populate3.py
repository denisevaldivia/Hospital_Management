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
                    if not row['id_paciente'] or not row['id_servicio']:  # Skip empty or incomplete rows
                        continue
                    servicio = {
                        'uid': '_:' + row['id_paciente'],
                        'id_paciente': int(row['id_paciente']),
                        'id_servicio': int(row['id_servicio']),
                        'nombre_servicio': row['nombre_servicio'],
                        'dgraph.type': 'Servicio'
                    }
                    servicios.append(servicio)
                if servicios:
                    resp = txn.mutate(set_obj=servicios)
            txn.commit()
        finally:
            txn.discard()

        return resp.uids if resp else {}

    def load_transacciones(file_path):
        txn = client.txn()
        resp = None
        try:
            transacciones = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row['id_transaccion'] or not row['id_servicio']:  # Skip empty or incomplete rows
                        continue
                    transaccion = {
                        'uid': '_:' + row['id_transaccion'],
                        'id_transaccion': int(row['id_transaccion']),
                        'id_servicio': int(row['id_servicio']),
                        'nombre_servicio': row['nombre_servicio'],
                        'precio': float(row['precio']) if row['precio'] else 0.0,
                        'dgraph.type': 'Transaccion'
                    }
                    transacciones.append(transaccion)
                if transacciones:
                    resp = txn.mutate(set_obj=transacciones)
            txn.commit()
        finally:
            txn.discard()

        return resp.uids if resp else {}

    def load_pacientes(file_path):
        txn = client.txn()
        resp = None
        try:
            pacientes = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row['id_paciente']:  # Skip rows without patient ID
                        continue
                    paciente = {
                        'uid': '_:' + row['id_paciente'],
                        'id_paciente': int(row['id_paciente']),
                        'nombre': row['nombre'],
                        'sexo': row['sexo'],
                        'edad': int(row['edad']) if row['edad'] else 0,
                        'fecha_nacimiento': pd.to_datetime(row['fecha_nacimiento']).isoformat() if row['fecha_nacimiento'] else None,
                        'dgraph.type': 'Paciente'
                    }
                    pacientes.append(paciente)
                if pacientes:
                    resp = txn.mutate(set_obj=pacientes)
            txn.commit()
        finally:
            txn.discard()
        return resp.uids if resp else {}

    def load_salas(file_path):
        txn = client.txn()
        resp = None
        try:
            salas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row['id_sala'] or not row['id_doctor']:  # Skip rows with missing sala/doctor
                        continue
                    sala = {
                        'uid': '_:' + row['id_sala'],
                        'id_sala': int(row['id_sala']),
                        'id_doctor': int(row['id_doctor']),
                        'tipo': row['tipo'],
                        'dgraph.type': 'Sala'
                    }
                    salas.append(sala)
                if salas:
                    resp = txn.mutate(set_obj=salas)
            txn.commit()
        finally:
            txn.discard()

        return resp.uids if resp else {}

    def load_doctores(file_path):
        txn = client.txn()
        resp = None
        try:
            doctores = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row['id_doctor']:  # Skip rows without doctor ID
                        continue
                    doctor = {
                        'uid': '_:' + row['id_doctor'],
                        'id_doctor': int(row['id_doctor']),
                        'nombre': row['nombre'],
                        'especialidad': row['especialidad'],
                        'licencia': row['licencia'],
                        'anios_experiencia': int(row['anios_experiencia']) if row['anios_experiencia'] else 0,
                        'correo': row['correo'],
                        'telefono': int(row['telefono']) if row['telefono'] else 0,
                        'dgraph.type': 'Doctor'
                    }
                    doctores.append(doctor)
                if doctores:
                    resp = txn.mutate(set_obj=doctores)
            txn.commit()
        finally:
            txn.discard()

        return resp.uids if resp else {}

    def load_visitas(file_path):
        txn = client.txn()
        resp = None
        try:
            visitas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row['id_paciente']:  # Skip rows with missing patient ID
                        continue
                    visita = {
                        'uid': '_:' + row['id_paciente'],
                        'id_paciente': int(row['id_paciente']),
                        'id_visita': int(row['id_visita']),
                        'nombre_visitante': row['nombre_visitante'],
                        'relacion_paciente': row['relacion_paciente'],
                        'motivo': row['motivo'],
                        'hora_entrada': pd.to_datetime(row['hora_entrada']).isoformat() if row['hora_entrada'] else None,
                        'hora_salida': pd.to_datetime(row['hora_salida']).isoformat() if row['hora_salida'] else None,
                        'dgraph.type': 'Visita'
                    }
                    visitas.append(visita)
                if visitas:
                    resp = txn.mutate(set_obj=visitas)
            txn.commit()
        finally:
            txn.discard()

        return resp.uids if resp else {}

    def load_recetas(file_path):
        txn = client.txn()
        resp = None
        try:
            recetas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row['id_receta']:  # Skip rows without receta ID
                        continue
                    receta = {
                        'uid': '_:' + row['id_receta'],
                        'id_receta': int(row['id_receta']),
                        'id_paciente': int(row['id_paciente']),
                        'id_doctor': int(row['id_doctor']),
                        'diagnostico': row['diagnostico'],
                        'fecha_emision': pd.to_datetime(row['fecha_emision']).isoformat() if row['fecha_emision'] else None,
                        'medicina': row['medicina'],
                        'cantidad': int(row['cantidad']) if row['cantidad'] else 0,
                        'frecuencia': int(row['frecuencia']) if row['frecuencia'] else 0,
                        'dgraph.type': 'Receta'
                    }
                    recetas.append(receta)
                if recetas:
                    resp = txn.mutate(set_obj=recetas)
            txn.commit()
        finally:
            txn.discard()

        return resp.uids if resp else {}

    # Create edges (unchanged)
    def create_edge(client, file_path, from_uids, to_uids, predicado, col_from, col_to):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    f_uid = from_uids.get(row[col_from], None)
                    t_uid = to_uids.get(row[col_to], None)
                    if not f_uid or not t_uid:  # Skip if either UID is missing
                        continue
                    mutation = {
                        'uid': f_uid,
                        predicado: { 'uid': t_uid }
                    }

                    print(f"Creando relacion {f_uid} -{predicado}-> {t_uid}")
                    txn.mutate(set_obj=mutation)

            txn.commit()
        finally:
            txn.discard()

    # Paths
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    NODES_DIR = os.path.join(ROOT_DIR, 'csv_data/nodos')
    EDGES_DIR = os.path.join(ROOT_DIR, 'csv_data/relaciones')

    # ====================================================================================
    # LOAD (same order, now SAFE)
    # ====================================================================================
    pacientes_uid     = load_pacientes(os.path.join(NODES_DIR, 'pacientes.csv'))
    doctores_uid      = load_doctores(os.path.join(NODES_DIR, 'doctores.csv'))
    salas_uid         = load_salas(os.path.join(NODES_DIR, 'salas.csv'))
    servicios_uid     = load_servicios(os.path.join(NODES_DIR, 'servicios.csv'))
    transacciones_uid = load_transacciones(os.path.join(NODES_DIR, 'transacciones.csv'))
    visitas_uid       = load_visitas(os.path.join(NODES_DIR, 'visitas.csv'))
    recetas_uid       = load_recetas(os.path.join(NODES_DIR, 'recetas.csv'))

    # ====================================================================================
    # RELATIONSHIPS (two corrections: visitas uses id_visita → id_paciente)
    # ====================================================================================
    create_edge(client, os.path.join(EDGES_DIR, 'agenda_doctor_sala.csv'), doctores_uid, salas_uid, "AGENDA", "id_doctor", "id_sala")
    create_edge(client, os.path.join(EDGES_DIR, 'atiende_doctor_paciente.csv'), doctores_uid, pacientes_uid, "ATIENDE", "id_doctor", "id_paciente")
    create_edge(client, os.path.join(EDGES_DIR, 'genera_servicio_transaccion.csv'), servicios_uid, transacciones_uid, "GENERA", "id_servicio", "id_transaccion")
    create_edge(client, os.path.join(EDGES_DIR, 'otorga_doctor_receta.csv'), doctores_uid, recetas_uid, "OTORGA", "id_doctor", "id_receta")
    create_edge(client, os.path.join(EDGES_DIR, 'recibe_paciente_visita.csv'), pacientes_uid, visitas_uid, "RECIBE", "id_paciente", "id_visita")
    create_edge(client, os.path.join(EDGES_DIR, 'solicita_paciente_servicio.csv'), pacientes_uid, servicios_uid, "SOLICITA", "id_paciente", "id_servicio")
    create_edge(client, os.path.join(EDGES_DIR, 'tiene_paciente_receta.csv'), pacientes_uid, recetas_uid, "TIENE", "id_paciente", "id_receta")

    print("Datos cargados correctamente.")
