from pyspark.sql.functions import col, substring, row_number
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_vrijeme_dim(date_df, csv_date_df=None):
    spark = get_spark_session()

    def prepare_df(df):
        return (
            df
            .withColumn("timestamp_str", col("timestamp").cast("string")) # Osiguravanje da je datum string zbog problema s vremenskim zonama
            .select(
                col("timestamp_str").alias("timestamp"),
                col("is_weekend").cast("boolean")
            )
            .withColumn("year", substring("timestamp", 1, 4).cast("int"))
            .withColumn("month", substring("timestamp", 6, 2).cast("int"))
            .withColumn("day", substring("timestamp", 9, 2).cast("int"))
            .withColumn("hour", substring("timestamp", 12, 2).cast("int"))
        )

    # Baza podataka
    db_df = prepare_df(date_df)

    # CSV podaci
    if csv_date_df:
        csv_df = prepare_df(csv_date_df)
        combined_df = db_df.unionByName(csv_df)
    else:
        combined_df = db_df

    # Definiranje finalne tablice
    window = Window.orderBy("year", "month", "day", "hour")
    final_df = (
        combined_df
        .withColumn("vrijeme_tk", row_number().over(window))
        .select(
            "vrijeme_tk",
            "year",
            "month",
            "day",
            "hour",
            "is_weekend"
        )
    )

    # Provjera broja podataka
    assert final_df.count() == 50000, "Expected 50000 rows in dim_vrijeme."

    return final_df
