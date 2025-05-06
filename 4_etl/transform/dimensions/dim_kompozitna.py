from pyspark.sql.functions import col, row_number, lit, current_timestamp
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_kompozitna_dim(composite_df, csv_composite_df=None):
    
    # Baza podataka
    db_df = (
        composite_df
        .select(
            col("ip_address_flag").cast("boolean"),
            col("previous_fraudulent_activity").cast("boolean")
        )
    )

    # CSV podaci
    if csv_composite_df:
        csv_df = (
            csv_composite_df
            .select(
                col("ip_address_flag").cast("boolean"),
                col("previous_fraudulent_activity").cast("boolean")
            )
        )

        combined_df = db_df.unionByName(csv_df)

    # Brisanje duplikata
    combined_df = combined_df.dropDuplicates(["ip_address_flag", "previous_fraudulent_activity"])

    # Definiranje finalne tablice
    window = Window.orderBy("ip_address_flag", "previous_fraudulent_activity")
    final_df = (
        combined_df
        .withColumn("kompozitna_tk", row_number().over(window))
        .select(
            "kompozitna_tk",
            "ip_address_flag",
            "previous_fraudulent_activity"
        )
    )

    # Provjera broja podataka
    assert final_df.count() == 4, "Number of composits from step one of the project."

    return final_df
