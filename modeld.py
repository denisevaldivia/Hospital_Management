# ------------------------------------
#   DGRAPH MODELO
# ------------------------------------
import pydgraph

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

        GENERAN: uid @reverse .
        SOLICITAN: [uid] @reverse .
        RECIBEN: [uid] @reverse .
        ATIENDEN: [uid] @reverse .
        AGENDA: [uid] @reverse .
        TIENEN: [uid] @reverse .
        OTORGAN: [uid] @reverse .

        
        type Transaccion {
            id_transaccion
            nombre_servicio
            precio
            GENERAN
        }

        type Servicio {
            id_paciente
            nombre_servicio
            GENERAN
            SOLICITAN
        }

        type Paciente {
            id_paciente
            nombre
            sexo
            fecha_nacimiento
            edad
            ATIENDEN
            SOLICITAN
            RECIBEN
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
            ATIENDEN
            OTORGAN
        }

        type Visita {
            id_paciente
            nombre_visitante
            relacion_paciente
            motivo
            hora_entrada
            hora_salida
            RECIBEN
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
            TIENEN
            OTORGAN
        }
    """
    op = pydgraph.Operation(schema=schema)
    client.alter(op)
    print("[DGRAPH] Schema configurado correctamente")

# ------------------------------------
#   QUERIES VACÍAS
# ------------------------------------
def query_1(client): pass
def query_2(client): pass
def query_3(client): pass
def query_4(client): pass
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

