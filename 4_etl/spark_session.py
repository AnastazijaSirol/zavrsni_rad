from pyspark.sql import SparkSession

def get_spark_session(app_name="ETL Project"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.29") \
        .getOrCreate()
