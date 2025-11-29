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


## ** Instalación de virtual env (Window) **
python -m venv ./venv
.\venv\Scripts\Activate.ps1

## ** Instalación requirements **
pip install -r requirements.txt

## ** Run conteiners **

docker run --name cassandra-conteiner -p 9042:9042 -d cassandra

docker run --name dbmongo -d -p 27017:27017 mongo

docker run --name dgraph-conteiner -d -p 8080:8080 -p 9080:9080 dgraph/standalone:latest
docker run --name ratel-conteiner -d -p 8000:8000 dgraph/ratel:latest

## ** Correr el codigo main **
python main.py
