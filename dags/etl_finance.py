from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.models import Variable

from datetime import datetime, timedelta
from os import environ as env
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

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM finance_spark WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""

# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
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

def success_callback_function(context):
    dag_run = context.get("dag_run")
    msg = "DAG ran successfully"
    subject = f"DAG {dag_run} has completed"
    send_email(msg, subject)


def failure_callback_function(context):
    dag_run = context.get("dag_run")
    msg = "DAG ran failed"
    subject = f"DAG {dag_run} has failed"
    send_email(msg, subject)


def send_email(msg, subject):
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
    "start_date": datetime(2023, 7, 11),
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
    on_success_callback=success_callback_function,
    on_failure_callback=failure_callback_function,
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
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_finance 
