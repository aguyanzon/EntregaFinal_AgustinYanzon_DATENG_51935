from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import smtplib


QUERY_CREATE_TABLE = '''
CREATE TABLE IF NOT EXISTS finance_spark (
    "date_from" VARCHAR(30),
    "open" VARCHAR(30),
    "high" VARCHAR(30),
    "low" VARCHAR(30),
    "close" VARCHAR(30), 
    "volume" VARCHAR(30),
    "monthly variation" VARCHAR(30),
    process_date VARCHAR(10),
    symbol VARCHAR(30) distkey
) sortkey(date_from);
'''

QUERY_CLEAN_PROCESS_DATE = '''
DELETE FROM finance_spark WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
'''

QUERY_GET_LAST_VALUES_PER_SYMBOL = '''
SELECT fs.symbol, fs.date_from AS last_date_from, fs."close" AS last_close, fs."monthly variation" AS last_monthly_variation
FROM finance_spark fs
INNER JOIN (
    SELECT symbol, MAX(date_from) AS last_date
    FROM finance_spark
    GROUP BY symbol
) last_dates
ON fs.symbol = last_dates.symbol AND fs.date_from = last_dates.last_date;
'''

def get_process_date(**kwargs):
    """
    Obtiene la fecha de proceso del DAG y la almacena en XCom.
    """
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


def send_last_values_email(**kwargs):
    """
    Obtiene los últimos valores de acciones desde XCom y envía un correo 
    electrónico con la información.
    """
    ti = kwargs["ti"]
    last_values = ti.xcom_pull(task_ids="select_action")

    subject = "Ultimos valores de acciones"
    msg = "\n".join(f"La accion {data[0]} para la fecha {data[1]} tuvo un precio de cierre de {float(data[2]):.2f} USD y su variacion respecto al mes anterior es de {float(data[3]):.2f} %" for data in last_values)
    
    send_email(msg, subject)


def on_success_callback(context):
    """
    Función de devolución de llamada para ejecutar cuando el DAG se ejecuta correctamente.
    """
    dag_run = context.get("dag_run")
    msg = "DAG ran successfully"
    subject = f"DAG {dag_run} has completed"
    send_email(msg, subject)


def on_failure_callback(context):
    """
    Función de devolución de llamada para ejecutar cuando el DAG falla.
    """
    dag_run = context.get("dag_run")
    msg = "DAG ran failed"
    subject = f"DAG {dag_run} has failed"
    send_email(msg, subject)


def send_email(msg, subject):
    """
    Envía un correo electrónico utilizando el servidor SMTP de Gmail.
    """
    try:
        x = smtplib.SMTP("smtp.gmail.com", 587)
        x.starttls()
        x.login(Variable.get("SMTP_EMAIL_FROM"), Variable.get("SMTP_PASSWORD"))

        message = "Subject: {}\n\n{}".format(subject, msg)
        x.sendmail(
            Variable.get("SMTP_EMAIL_FROM"), Variable.get("SMTP_EMAIL_TO"), message
        )
        print("Exito al enviar el mail")
    except Exception as exception:
        print(exception)
        print("Fallo al enviar el mail")


defaul_args = {
    "owner": "Agustin Yanzón",
    "start_date": datetime(2023, 7, 27),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    "catchup": False,
}

with DAG(
    dag_id="etl_finance",
    default_args=defaul_args,
    description="ETL de la tabla finance",
    schedule_interval="@daily",
    catchup=False,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
) as dag:

    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
    )

    spark_etl_finance = SparkSubmitOperator(
        task_id="spark_etl_finance",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_Finance.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        do_xcom_push=True,
    )

    get_last_values_per_symbol = SQLExecuteQueryOperator(
        task_id="get_last_values_per_symbol",
        conn_id="redshift_default",
        sql=QUERY_GET_LAST_VALUES_PER_SYMBOL,
        dag=dag,
        do_xcom_push=True,
    )

    send_last_values_email_task = PythonOperator(
        task_id="send_last_values_email",
        python_callable=send_last_values_email,
        provide_context=True,
        dag=dag,
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_finance
    spark_etl_finance >> get_last_values_per_symbol >> send_last_values_email_task
