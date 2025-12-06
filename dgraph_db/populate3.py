import datetime
import json
import csv
import pydgraph
import pandas as pd
import os

def populate_data(client):

    # ====================================================================================
    # FIX 1: Helper to generate SAFE blank node ids with unique prefixes for each type
    # ====================================================================================
    def make_uid(prefix, pk):
        return f"_:{prefix}{pk}"

    # ====================================================================================
    # FIX 2: All loaders now use unique prefixes instead of reusing raw ids
    # ====================================================================================

    def load_servicios(file_path):
        txn = client.txn()
        resp = None
        uid_map = {}  # FIX
        try:
            servicios = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    blank = make_uid("serv_", row["id_paciente"])  # FIX
                    servicios.append({
                        'uid': blank,
                        'id_paciente': int(row['id_paciente']),
                        'nombre_servicio': row['nombre_servicio'],
                        'dgraph.type': 'Servicio'
                    })
                resp = txn.mutate(set_obj=servicios)
            txn.commit()

            # Build mapping: primary key → real UID
            for b, real_uid in resp.uids.items():
                pk = b.split("serv_")[1]
                uid_map[pk] = real_uid

        finally:
            txn.discard()

        return uid_map


    def load_transacciones(file_path):
        txn = client.txn()
        resp = None
        uid_map = {}
        try:
            transacciones = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    blank = make_uid("tran_", row["id_transaccion"])  # FIX
                    transacciones.append({
                        'uid': blank,
                        'id_transaccion': int(row['id_transaccion']),
                        'nombre_servicio': row['nombre_servicio'],
                        'precio': float(row['precio']),
                        'dgraph.type': 'Transaccion'
                    })
                resp = txn.mutate(set_obj=transacciones)
            txn.commit()

            for b, real_uid in resp.uids.items():
                pk = b.split("tran_")[1]
                uid_map[pk] = real_uid

        finally:
            txn.discard()

        return uid_map


    def load_pacientes(file_path):
        txn = client.txn()
        resp = None
        uid_map = {}
        try:
            pacientes = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    blank = make_uid("pac_", row["id_paciente"])  # FIX

                    pacientes.append({
                        'uid': blank,
                        'id_paciente': int(row['id_paciente']),
                        'nombre': row['nombre'],
                        'sexo': row['sexo'],
                        'edad': int(row['edad']),
                        'fecha_nacimiento': pd.to_datetime(row['fecha_nacimiento']).isoformat(),
                        'dgraph.type': 'Paciente'
                    })
                resp = txn.mutate(set_obj=pacientes)
            txn.commit()

            for b, real_uid in resp.uids.items():
                pk = b.split("pac_")[1]
                uid_map[pk] = real_uid

        finally:
            txn.discard()

        return uid_map


    def load_salas(file_path):
        txn = client.txn()
        resp = None
        uid_map = {}
        try:
            salas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    blank = make_uid("sala_", row["id_sala"])  # FIX
                    salas.append({
                        'uid': blank,
                        'id_sala': int(row['id_sala']),
                        'id_doctor': int(row['id_doctor']),
                        'tipo': row['tipo'],
                        'dgraph.type': 'Sala'
                    })
                resp = txn.mutate(set_obj=salas)
            txn.commit()

            for b, real_uid in resp.uids.items():
                pk = b.split("sala_")[1]
                uid_map[pk] = real_uid

        finally:
            txn.discard()

        return uid_map


    def load_doctores(file_path):
        txn = client.txn()
        resp = None
        uid_map = {}
        try:
            doctores = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    blank = make_uid("doc_", row["id_doctor"])  # FIX
                    doctores.append({
                        'uid': blank,
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

            for b, real_uid in resp.uids.items():
                pk = b.split("doc_")[1]
                uid_map[pk] = real_uid

        finally:
            txn.discard()

        return uid_map


    def load_visitas(file_path):
        txn = client.txn()
        resp = None
        uid_map = {}  
        try:
            visitas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    blank = make_uid("vis_", row["id_visita"])  # FIX (DO NOT USE id_paciente)

                    visitas.append({
                        'uid': blank,
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

            for b, real_uid in resp.uids.items():
                pk = b.split("vis_")[1]
                uid_map[pk] = real_uid

        finally:
            txn.discard()

        return uid_map


    def load_recetas(file_path):
        txn = client.txn()
        resp = None
        uid_map = {}
        try:
            recetas = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    blank = make_uid("rec_", row["id_receta"])  # FIX
                    recetas.append({
                        'uid': blank,
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

            for b, real_uid in resp.uids.items():
                pk = b.split("rec_")[1]
                uid_map[pk] = real_uid

        finally:
            txn.discard()

        return uid_map

    # ====================================================================================
    # FIX 3: EDGE CREATOR – no change, but relies on corrected maps
    # ====================================================================================
    def create_edge(client, file_path, from_uids, to_uids, predicado, col_from, col_to):
        txn = client.txn()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    f_uid = from_uids[row[col_from]]
                    t_uid = to_uids[row[col_to]]

                    mutation = {
                        'uid': f_uid,
                        predicado: {'uid': t_uid}
                    }

                    print(f"Creando relacion {f_uid} -{predicado}-> {t_uid}")
                    txn.mutate(set_obj=mutation)

            txn.commit()
        finally:
            txn.discard()


    # ====================================================================================
    # FIX 4: Paths unchanged
    # ====================================================================================
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
    create_edge(client, os.path.join(EDGES_DIR, 'genera_servicio_transaccion.csv'), servicios_uid, transacciones_uid, "GENERA", "id_paciente", "id_transaccion")
    create_edge(client, os.path.join(EDGES_DIR, 'otorga_doctor_receta.csv'), doctores_uid, recetas_uid, "OTORGA", "id_doctor", "id_receta")

    # FIX: visita now references correct id_visita
    create_edge(client, os.path.join(EDGES_DIR, 'recibe_paciente_visita.csv'), pacientes_uid, visitas_uid, "RECIBE", "id_paciente", "id_visita")

    create_edge(client, os.path.join(EDGES_DIR, 'solicita_paciente_servicio.csv'), pacientes_uid, servicios_uid, "SOLICITA", "id_paciente", "id_paciente")
    create_edge(client, os.path.join(EDGES_DIR, 'tiene_paciente_receta.csv'), pacientes_uid, recetas_uid, "TIENE", "id_paciente", "id_receta")

    print("Datos cargados correctamente.")
