from os import environ as env
from psycopg2 import connect
from pyspark.sql import SparkSession

# Redshift configuration variables
REDSHIFT_HOST = env["REDSHIFT_HOST"]
REDSHIFT_PORT = env["REDSHIFT_PORT"]
REDSHIFT_DB = env["REDSHIFT_DB"]
REDSHIFT_USER = env["REDSHIFT_USER"]
REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
REDSHIFT_URL = env["REDSHIFT_URL"]


class ETL_Spark:
    # Path to the Postgres driver for Spark (JDBC) (Also used for Redshift)
    DRIVER_PATH = env["DRIVER_PATH"]
    JDBC_DRIVER = "org.postgresql.Driver"

    def __init__(self, job_name=None):
        """
        Class constructor, initializes the Spark session
        """
        print(">>> [init] Initializing ETL...")

        env["SPARK_CLASSPATH"] = self.DRIVER_PATH

        env[
            "PYSPARK_SUBMIT_ARGS"
        ] = f"--driver-class-path {self.DRIVER_PATH} --jars {self.DRIVER_PATH} pyspark-shell"

        # Create Spark session
        self.spark = (
            SparkSession.builder.master("local[1]")
            .appName("ETL Spark" if job_name is None else job_name)
            .config("spark.jars", self.DRIVER_PATH)
            .config("spark.executor.extraClassPath", self.DRIVER_PATH)
            .getOrCreate()
        )

        try:
            # Connect to Redshift
            print(">>> [init] Connecting to Redshift...")
            self.conn_redshift = connect(
                host=REDSHIFT_HOST,
                port=REDSHIFT_PORT,
                database=REDSHIFT_DB,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
            )
            self.cur_redshift = self.conn_redshift.cursor()
            print(">>> [init] Connection successful")
            # Cerrar la conexión
            self.cur_redshift.close()
            self.conn_redshift.close()
        except:
            print(">>> [init] Unable to connect to Redshift")

    def execute(self, process_date: str):
        """
        Main method that executes the ETL process

        Args:
            process_date (str): Process date in format YYYY-MM-DD
        """
        print(">>> [execute] Running ETL...")

        # Extract data from the API
        df_api = self.union_data()

        # Transform the data
        df_transformed = self.transform(df_api)

        # Load the transformed data into Redshift
        self.load(df_transformed)

    def extract(self):
        """
        Extract data from the API
        """
        print(">>> [E] Extracting data from the API...")

    def union_data(self):
        """
        Combine data from the API
        """
        print(">>> [E] Concatenating DataFrames...")

    def transform(self, df_original):
        """
        Transform the data
        """
        print(">>> [T] Transforming data...")

    def load(self, df_final):
        """
        Load the transformed data into Redshift
        """
        print(">>> [L] Loading data into Redshift...")

