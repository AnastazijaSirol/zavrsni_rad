from pyspark.sql.functions import col, trim, initcap, lit, row_number, current_timestamp, when
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, LongType

def transform_trgovac_dim(merchant_df, csv_merchant_df=None):
    # Baza podataka
    db_merchant_df = (
        merchant_df
        .select(
            col("id").cast(LongType()).alias("merchant_id"),
            initcap(trim(col("merchant_category"))).alias("merchant_category")
        )
    )

    # CSV podaci
    if csv_merchant_df:
        csv_df = (
            csv_merchant_df
            .selectExpr("merchant_category")
            .withColumn("merchant_category", initcap(trim(col("merchant_category"))))
            .withColumn("merchant_id", lit(None).cast(LongType()))
        )
        db_merchant_df = db_merchant_df.unionByName(csv_df)

    # Brisanje duplikata
    db_merchant_df = db_merchant_df.dropDuplicates(["merchant_category"])

    # Definiranje tipa trgovca
    db_merchant_df = db_merchant_df.withColumn(
        "merchant_type",
        when(col("merchant_category").isin("Groceries"), "Everyday")
        .when(col("merchant_category").isin("Travel", "Restaurants"), "Luxury")
        .when(col("merchant_category").isin("Electronics", "Clothing"), "Consumable")
        .otherwise("Unknown")
    )

    # Definiranje finalne tablice
    window = Window.orderBy("merchant_category")
    final_df = (
        db_merchant_df
        .withColumn("merchant_tk", row_number().over(window))
        .select(
            "merchant_tk",
            "merchant_id",
            "merchant_category",
            "merchant_type"
        )
    )

    # Provjera broja podataka
    assert final_df.count() == 5, "Number of merchants from step one of the project."

    return final_df
