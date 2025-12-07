# Hospital_Management

### **Integrantes**:
- Diana Denise Valdivia Vargas 752959
- Viviana Toledo de la Fuente 751635
- José Emanuel Pulido Tinajero 752181
- Lilia Alejandra Padilla Hernández 673256

## **Descripción**
Desarrollar una aplicación que interactúe con MongoDB, Dgraph y Cassandra, demostrando cómo trabajar con modelos de datos documentales, de grafos y de columnas amplias en un escenario del mundo real.

## **Justificación**
El proyecto busca desarrollar una Plataforma de Integración de Datos de Salud que centralice y consolide información hospitalaria diversa, facilitando su gestión administrativa y logística. La intención del equipo es mejorar la eficiencia operativa del hospital, optimizando la asignación de recursos, la ocupación de salas, el control de insumos, el flujo de personal y los registros históricos de actividades y eventos.

El objetivo final es proporcionar a los administradores y personal de operaciones datos accesibles y confiables que permitan tomar decisiones informadas para planificar recursos, programar turnos, controlar inventarios y mantener la calidad y seguridad de los servicios hospitalarios. La plataforma permitirá un monitoreo de la operación hospitalaria, contribuyendo a la eficiencia, seguimiento y control de todos los procesos administrativos y logísticos.

## **Funcionamiento**

Nuestra aplicación implementa tres bases de datos no relacionales: CassandraDB, MongoDB y Dgraph para administrar las operaciones dentro de un hospital. 

Las bases de datos y las consultas son independientes entre sí, lo que quiere decir que no requieren estar activas al mismo tiempo, y es posible interactuar con solo una de ellas.

La aplicación cuenta con esquemas para cada base de datos, así como consultas puntuales, que permitirán administrar las operaciones del hospital. 

## **Instalación de virtual env (Windows)**
python -m venv ./venv

.\venv\Scripts\Activate.ps1


## **Instalar los requerimientos**
pip install -r requirements.txt


## **Ejecutar los contenedores**

Nuestra aplicación depende de contenedores de Docker para conectarse a las tres bases de datos implementadas. En este sentido, hay dos maneras de ejecutar los contenedores, ya sea individualmente, o de manera grupal con el Docker Compose. 

Sugerimos el Compose si se van a utilizar las 3 bases de datos a la vez, y la ejecución individual si solo quiere consultarse una base de datos.

### • **Correr cada contenedor individualmente**

docker run --name cassandra-conteiner -p 9042:9042 -d cassandra

docker run --name dbmongo -d -p 27017:27017 mongo

docker run --name dgraph-conteiner -d -p 8080:8080 -p 9080:9080 dgraph/standalone:latest

docker run --name ratel-conteiner -d -p 8000:8000 dgraph/ratel:latest

### • **Correr los contenedores con Docker Compose**

Para la primera ejecución:

&ensp; docker compose up --build

Para las ejecuciones posteriores:

&ensp; docker compose up

## **Correr el codigo main**
python main.py
