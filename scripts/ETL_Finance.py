# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users

import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col, lit, when, expr, to_date, max, lag

from commons import ETL_Spark

class ETL_Finance(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self, symbol):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")

        try:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={env["API_KEY"]}'
            response = requests.get(url)
            json_data = response.json()["Monthly Time Series"]

            df = (
                self.spark.sparkContext.parallelize(list(json_data.items()))
                .map(lambda x: Row(date=x[0], **x[1]))
                .toDF(["date_from", "open", "high", "low", "close", "volume"])
            )

            df = df.withColumn("symbol", lit(symbol))

            return df

        except requests.exceptions.RequestException as e:
            print(f"Error de solicitud: {e}")
            return None
    
    
    def union_data(self):
        print(">>> [E] Concatenando DataFrames...")

        data_ibm = self.extract('IBM')
        data_aapl = self.extract('AAPL')
        data_tsla = self.extract('TSLA')
        data = data_ibm.union(data_aapl).union(data_tsla)

        return data


    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        total_rows = df_original.count()
        distinct_rows = df_original.dropDuplicates().count()

        # Compara la cantidad de filas antes y después de eliminar los duplicados
        if total_rows == distinct_rows:
            print("El DataFrame no tiene duplicados.")
        else:
            print("El DataFrame tiene duplicados.")

        window_spec = Window.partitionBy('symbol').orderBy('date_from')
        df_original = df_original.withColumn('monthly variation', (col('close') - lag('close').over(window_spec)) / lag('close').over(window_spec) * 100)

        return df_original


    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        df_final = df_final.withColumn("process_date", lit(self.process_date))
        
        df_final.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.finance_spark") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(">>> [L] Datos cargados exitosamente")

        return df_final
    
    


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Finance()
    etl.run()
