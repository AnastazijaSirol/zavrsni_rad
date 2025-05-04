from pyspark.sql.functions import col, lit, current_timestamp, row_number, when
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

def transform_kartica_dim(card_df, user_df, csv_card_df=None):
    c = card_df.alias("c")
    u = user_df.alias("u")

    # Baza podataka
    db_cards_df = (
        c.join(u, col("c.user_fk") == col("u.id"), "left")
         .select(
             col("c.id").cast(StringType()).alias("card_id"),
             col("c.card_type").alias("card_type"),
             col("c.card_age").alias("card_age"),
             col("u.id").cast(StringType()).alias("user_id")
         )
    )
    # CSV podaci
    if csv_card_df:
        csv_cards_df = (
            csv_card_df
            .select(
                col("card_type"),
                col("card_age").cast("int"),
                col("user_id").cast(StringType())
            )
            .withColumn("card_id", lit(None).cast(StringType()))
        )

        db_cards_df = db_cards_df.select("card_id", "card_type", "card_age", "user_id") \
                                 .unionByName(csv_cards_df)

    # Definiranje kategorija starosti kartica
    db_cards_df = db_cards_df.withColumn(
        "card_age_category",
        when(col("card_age") < 60, "New")
        .when((col("card_age") >= 60) & (col("card_age") < 120), "Stable")
        .otherwise("Long term")
    )

    # Brisanje duplikata
    db_cards_df = db_cards_df.dropDuplicates(["card_type", "user_id", "card_age"])

    # Uklanjanje card_age
    db_cards_df = db_cards_df.drop("card_age")

    window = Window.orderBy("card_type", "user_id", "card_id")
    db_cards_df = (
        db_cards_df
        .withColumn("card_tk", row_number().over(window))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit(None).cast("timestamp"))
    )

    # Definiranje finalne tablice
    final_df = db_cards_df.select(
        "card_tk",
        "card_id",
        "card_type",
        "card_age_category",
        "user_id",
        "date_from",
        "date_to"
    )

    assert final_df.count() == 49848, "Number of cards from step one of the project."

    return final_df