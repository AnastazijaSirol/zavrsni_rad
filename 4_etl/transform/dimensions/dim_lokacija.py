from pyspark.sql.functions import col, lit, trim, initcap, row_number, current_timestamp, when
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, LongType

def transform_lokacija_dim(location_df, state_df, csv_location_df=None):
    l = location_df.alias("l")
    s = state_df.alias("s")

    # Baza podataka
    db_loc_df = (
        l.join(s, col("l.state_fk") == col("s.id"), "left")
         .select(
             col("l.id").cast(LongType()).alias("location_id"),
             initcap(trim(col("l.name"))).alias("city"),
             col("l.population").cast(LongType()),
             initcap(trim(col("s.name"))).alias("state")
         )
    )

    # CSV podaci
    if csv_location_df:
        csv_df = (
            csv_location_df
            .selectExpr("location as city")
            .withColumn("city", initcap(trim(col("city"))))
            .withColumn("location_id", lit(None).cast(LongType()))
            .withColumn("population", lit(None).cast(LongType()))
            .withColumn("state", lit(None).cast(StringType()))
        )

        db_loc_df = db_loc_df.unionByName(csv_df)

    # Deiniranje kategorija populacije
    db_loc_df = db_loc_df.withColumn(
        "population_category",
        when(col("population") >= 10_000_000, "Mega City")
        .when(col("population") >= 1_000_000, "Large City")
        .when(col("population") >= 100_000, "Medium City")
        .when(col("population") > 0, "Small City")
        .otherwise("Unknown")
    )

    # Brisanje duplikata
    db_loc_df = db_loc_df.dropDuplicates(["city", "state", "population_category"])

    window = Window.orderBy("state", "city")
    db_loc_df = (
        db_loc_df
        .withColumn("location_tk", row_number().over(window))
    )

    # Definiranje finalne tablice
    final_df = db_loc_df.select(
        "location_tk",
        "location_id",
        "city",
        "population_category",
        "state",
    )

    # Provjera broja podataka
    assert final_df.count() == 16, "Number of locations from step one of the project."

    return final_df

    
