# ------------------------------------
#   DGRAPH MODELO
# ------------------------------------
import pydgraph
import json
import os
from pyvis.network import Network

def set_schema(client):
    schema = """
        
        id_transaccion: int @index(int) .
        id_paciente: int @index(int) .
        id_sala: int @index(int) .
        id_doctor: int @index(int) .
        id_receta: int @index(int) .

        nombre_servicio: string @index(term) .
        precio: float @index(float) .

        nombre: string .
        sexo: string @index(hash) .
        fecha_nacimiento: dateTime .
        edad: int @index(int) .

        tipo: string @index(term) .
        especialidad: string @index(hash) .
        licencia: string @index(hash) .
        anios_experiencia: int @index(int) .
        correo: string .
        telefono: int .

        nombre_visitante: string .
        relacion_paciente: string @index(hash) .
        motivo: string @index(term) .
        hora_entrada: dateTime @index(hour) .
        hora_salida: dateTime @index(hour) .

        diagnostico: string @index(term) .
        fecha_emision: dateTime @index(day) .
        medicina: string @index(term) .
        cantidad: int .
        frecuencia: int .

        GENERA: uid @reverse .
        SOLICITA: [uid] @reverse .
        RECIBE: [uid] @reverse .
        ATIENDE: [uid] @reverse .
        AGENDA: [uid] @reverse .
        TIENE: [uid] @reverse .
        OTORGA: [uid] @reverse .

        
        type Transaccion {
            id_transaccion
            nombre_servicio
            precio
            GENERA
        }

        type Servicio {
            id_paciente
            nombre_servicio
            GENERA
            SOLICITA
        }

        type Paciente {
            id_paciente
            nombre
            sexo
            fecha_nacimiento
            edad
            ATIENDE
            SOLICITA
            RECIBE
        }

        type Sala {
            id_sala
            id_doctor
            tipo
            AGENDA
        }

        type Doctor {
            id_doctor
            nombre
            especialidad
            licencia
            anios_experiencia
            correo
            telefono
            AGENDA
            ATIENDE
            OTORGA
        }

        type Visita {
            id_paciente
            nombre_visitante
            relacion_paciente
            motivo
            hora_entrada
            hora_salida
            RECIBE
        }

        type Receta {
            id_receta
            id_paciente
            id_doctor
            diagnostico
            fecha_emision
            medicina
            cantidad
            frecuencia
            TIENE
            OTORGA
        }
    """
    op = pydgraph.Operation(schema=schema)
    client.alter(op)
    print("[DGRAPH] Schema configurado correctamente")

# Query 4: Show the relationship between a patient and their doctors
def query_4(client, id_paciente): 
    query = """
    query getDoctors($id_paciente: int) {
        patient(func: eq(id_paciente, $id_paciente)) {
            uid
            paciente_id
            nombre
            sexo
            fecha_nacimiento
            edad
            doctores: ~ATIENDE {
                doctor_id
                nombre
                especialidad
                licencia
                anios_experiencia
                correo
                telefono
            }
        }
    }
    """

    # Fill out the query
    variables = {"$id_paciente": id_paciente}

    # Execute the response
    txn = client.txn(read_only=True)
    try:
        # Store the response
        res = txn.query(query, variables=variables)
        data = json.loads(res.json)
        patient_list = data.get('patient', [])

        # Visualize if data exists
        if patient_list:
            patient = patient_list[0]
            doctores = patient.get('doctores', [])
            
            print(f"Patient found: {patient.get('nombre')}")
            print(f"Number of doctors: {len(doctores)}")
            
            # Visualize the relationship
            visualize_dgraph_response(res, f"patient_{id_paciente}_doctors")
            
            return {"patient": patient, "doctores": doctores}
        else:
            print(f"No patient found with ID: {id_paciente}")
            return None

    finally:
        txn.discard()

# ------------------------------------
#   VISUALIZADOR
# ------------------------------------

# Visualize any query response in PyVis
def visualize_dgraph_response(query_response, output_name):    
    # Create folder to store visualizations
    ACTIVE_DIR = os.path.dirname(os.path.abspath(__file__))     #dgraph_db
    reports_dir = os.path.join(ACTIVE_DIR, 'reports')
    os.makedirs(reports_dir, exist_ok = True)
    
    # Output graph name
    output_file = f"{reports_dir}/{output_name}.html"

    # Parse the JSON response
    data = json.loads(query_response.json)
    
    # Create network
    net = Network(height="750px", width="100%", directed=True)
    
    # Simple colors for different node types
    colors = {
        "patient": "#8ec7ff",
        "doctor": "#ffd08e", 
        "default": "#e0e0e0",
    }
    
    # Recursive function to add nodes and edges
    def add_nodes_edges(data_dict, parent_uid=None, parent_type=None):
        if not isinstance(data_dict, dict):
            return
        
        for node_type, nodes in data_dict.items():
            if not isinstance(nodes, list):
                nodes = [nodes]
            
            for node in nodes:
                if isinstance(node, dict) and "uid" in node:
                    uid = node["uid"]
                    
                    # Determine node type for coloring
                    node_color = colors.get(node_type.lower(), colors["default"])
                    
                    # Create label from name or type
                    label = node.get("nombre") or node.get("name") or node_type
                    
                    # Add node
                    net.add_node(uid, label=label, color=node_color, 
                                title=f"{node_type}: {str(node)[:100]}...")
                    
                    # Add edge from parent if exists
                    if parent_uid:
                        net.add_edge(parent_uid, uid, label=parent_type)
                    
                    # Process relationships (nested dicts except uid)
                    for key, value in node.items():
                        if key != "uid" and isinstance(value, (dict, list)):
                            if isinstance(value, dict):
                                value = [value]
                            for item in value:
                                if isinstance(item, dict):
                                    add_nodes_edges({key: item}, uid, key)
    
    # Start processing
    add_nodes_edges(data)
    
    # Save and open
    net.save_graph(output_file)
    print(f"Graph saved as: {output_file}")
    
    return net, output_file

# ------------------------------------
#   QUERIES VACÍAS
# ------------------------------------
def query_1(client): pass
def query_2(client): pass
def query_3(client): pass
def query_5(client): pass
def query_6(client): pass
def query_7(client): pass
def query_8(client): pass
def query_9(client): pass
def query_10(client): pass
def query_11(client): pass
def query_12(client): pass
def query_13(client): pass
def query_14(client): pass

# HACER 4, 5 Y 9