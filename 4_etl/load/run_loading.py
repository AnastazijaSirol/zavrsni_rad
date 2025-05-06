from pyspark.sql import DataFrame

def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "append"):
    jdbc_url = "jdbc:mysql://127.0.0.1:3306/dw_dimensional?useSSL=false&serverTimezone=UTC"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver",
        "connectionTimeZone": "UTC",
        "zeroDateTimeBehavior": "convertToNull"
    }

    print(f"Writing to table `{table_name}` with mode `{mode}`...")

    spark_df.coalesce(1).write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=connection_properties
    )
    print(f"✅ Done writing to `{table_name}`.")