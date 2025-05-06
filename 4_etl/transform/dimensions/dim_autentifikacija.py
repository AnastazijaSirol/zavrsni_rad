from pyspark.sql.functions import col, trim, initcap, lit, row_number, current_timestamp, when
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, LongType

def transform_autentifikacija_dim(authentication_df, csv_authentication_df=None):
    # Baza podataka
    db_auth_df = (
        authentication_df
        .select(
            col("id").cast(LongType()).alias("authentication_id"),
            initcap(trim(col("authentication_method"))).alias("authentication_method")
        )
    )

    # CSV podaci
    if csv_authentication_df:
        csv_df = (
            csv_authentication_df
            .selectExpr("authentication_method")
            .withColumn("authentication_method", initcap(trim(col("authentication_method"))))
            .withColumn("authentication_id", lit(None).cast(LongType()))
        )

        db_auth_df = db_auth_df.unionByName(csv_df)

    # Brisanje duplikata
    db_auth_df = db_auth_df.dropDuplicates(["authentication_method"])

    # Definiranje kategorija metoda autentifikacija
    db_auth_df = db_auth_df.withColumn(
        "authentication_method_category",
        when(col("authentication_method").isin("Biometric", "Otp"), "Secure")
        .when(col("authentication_method").isin("Password", "Pin"), "Insecure")
        .otherwise("Unknown")
    )

    # Definiranje finalne tablice 
    window = Window.orderBy("authentication_method")
    final_df = (
        db_auth_df
        .withColumn("authentication_tk", row_number().over(window)) 
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit(None).cast("timestamp"))
        .select(
            "authentication_tk",
            "authentication_id",
            "authentication_method",
            "authentication_method_category",
            "date_from",
            "date_to"
        )
    )

    #Provjera broja podataka
    assert final_df.count() == 4, "Number of authentications from step one of the project."

    return final_df