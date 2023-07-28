# Proyecto Final Coderhouse Data Engineering

Este repositorio contiene un proyecto de Extracción, Transformación y Carga (ETL) que tiene como objetivo obtener datos del mercado financiero a través de una API en formato JSON, procesarlos mediante Apache Spark y finalmente cargar la información resultante en una tabla en Amazon Redshift. Todo el proceso está orquestado utilizando Apache Airflow, el cual además enviará notificaciones de los valores obtenidos al correo electrónico mediante el servicio SMTP.

## Desarrollo 
Para el siguiente trabajo se extrajeron los datos de
[Alpha Vantage](https://www.alphavantage.co/) que proporciona datos del mercado financiero de nivel empresarial. Para poder acceder a los datos es necesario generar una API_KEY que proporciona la página de manera gratuita.

En el proyecto se toman los valores mensuales (último día de negociación de cada mes, apertura mensual, máximo mensual, mínimo mensual, cierre mensual, volumen mensual) de las acciones de IBM (IBM), APPLE (AAPL) y TESLA (TSLA), que cubre más de 20 años de datos históricos.

## Distribución de los archivos
Los archivos a tener en cuenta son:
* `docker_images/`: Contiene los Dockerfiles para crear las imagenes utilizadas de Airflow y Spark.
* `docker-compose.yml`: Archivo de configuración de docker compose. Contiene la configuración de los servicios de Airflow y Spark.
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
* `dags/`: Carpeta con los archivos de los DAGs.
    * `etl_finance.py`: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios.
* `scripts/`: Carpeta con los scripts de Spark.
    * `postgresql-42.5.2.jar`: Driver de Postgres para Spark.
    * `common.py`: Script de Spark con funciones comunes.
    * `ETL_Finance.py`: Script de Spark que ejecuta el ETL.

## Instrucciones de Uso
### Clonar el repositorio
* Clona este repositorio en tu máquina local:
```bash
git clone https://github.com/aguyanzon/EntregaFinal_AgustinYanzon_DATENG_51935.git
```
### Generar imagen de Airflow
* Posicionarse en la carpeta `docker_images/airflow` y ejecutar el siguiente comando:
```bash
docker build -t aguyanzon/airflow:airflow_2_6_2 .
```
### Generar imagen de Spark
* Posicionarse en la carpeta `docker_images/spark` y ejecutar el siguiente comando:
```bash
docker build -t aguyanzon/spark:spark_3_4_1 .
```
### Variables de entorno
* Crear un archivo con variables de entorno llamado `.env` ubicado a la misma altura que el `docker-compose.yml`. Cuyo contenido sea:
```bash
REDSHIFT_HOST=
REDSHIFT_PORT=5439
REDSHIFT_DB=
REDSHIFT_USER=
REDSHIFT_SCHEMA=
REDSHIFT_PASSWORD=
API_KEY=
REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
```
### Levantar servicios de Airflow y Spark
* Posicionarse en la carpeta raíz del proyecto y ejecutar el siguiente comando:
```bash
docker-compose up --build
```
* ![Advertencia](https://img.shields.io/badge/-Advertencia-red)
En el caso de utilizar una Mac con Chip M1 probablemente sea necesario agregar la siguiente variable de entorno en el docker-compose en `airflow-common-env` para no tener inconvenientes con la arquitectura en Java `airflow-common-env`, caso contrario no es necesario.
```
JAVA_HOME: /usr/lib/jvm/java-11-openjdk-arm64/
```
### Configuración Airflow
* Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.
* En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
* En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Spark:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`
* En la pestaña `Admin -> Variables` crear las siguientes variables:
    * `driver_class_path` : /tmp/drivers/postgresql-42.5.2.jar
    * `spark_scripts_dir` : /opt/airflow/scripts
    * `SMTP_EMAIL_FROM` : dirección de correo electrónico del remitente para enviar alertas
    * `SMTP_EMAIL_TO` : dirección de correo electrónico del destinatario para recibir alertas
    * `SMTP_PASSWORD` : token de acceso para enviar mails ([Instrucciones](https://support.google.com/accounts/answer/185833?hl=es-419))

### Ejecución del DAG
* Una vez configurados todos los parámetros se debe ejecutar el DAG. El mismo realiza las siguientes tareas:
    * `get_process_date_task`: Esta tarea ejecuta la función get_process_date mediante el operador PythonOperator. Su objetivo es obtener la fecha de procesamiento actual y almacenarla para su uso en el resto del flujo.

    * `create_table`: Esta tarea ejecuta una consulta SQL mediante el operador SQLExecuteQueryOperator. Su función es crear una tabla en la base de datos Redshift para almacenar los datos procesados.

    * `clean_process_date`: Esta tarea ejecuta otra consulta SQL mediante el operador SQLExecuteQueryOperator. Su propósito es limpiar los datos de la fecha de procesamiento anterior para preparar el espacio para nuevos datos.

    * `spark_etl_finance`: Esta tarea ejecuta un script de Spark mediante el operador SparkSubmitOperator. El script de Spark, ubicado en el directorio especificado en la variable "spark_scripts_dir", se encarga de realizar la transformación de los datos financieros obtenidos.

    * `get_last_values_per_symbol`: Esta tarea ejecuta una consulta SQL mediante el operador SQLExecuteQueryOperator. Su objetivo es obtener los últimos valores por símbolo desde la base de datos Redshift y almacenarlos en el contexto de Airflow utilizando XCom.

    * `send_last_values_email_task`: Esta tarea ejecuta la función send_last_values_email mediante el operador PythonOperator. Su función es enviar un correo electrónico con los últimos valores obtenidos a los destinatarios especificados.

### Notificaciones
* El DAG incluye dos notificaciones por correo electrónico para mantener a los usuarios informados sobre el progreso y los resultados del proceso ETL.

    * #### Notificación de Estado del DAG
    Se envía una notificación de estado del DAG después de su ejecución. Si el DAG se ejecuta correctamente, recibirás un correo electrónico con el asunto "DAG [nombre_del_DAG] has completed", indicando que el proceso ETL se completó sin errores. En caso de que ocurra una falla durante la ejecución del DAG, recibirás un correo electrónico con el asunto "DAG [nombre_del_DAG] has failed", lo que te permitirá identificar y abordar rápidamente cualquier problema que haya ocurrido.

    * #### Notificación de Últimos Valores de Acciones
    También recibirás una notificación con los últimos valores de cierre y variación con respecto al mes anterior de las acciones. Esta notificación incluirá los datos detallados de cada acción y su precio de cierre en la última fecha registrada, así como la variación porcentual con respecto al mes anterior.
