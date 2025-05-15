from pyspark.sql.functions import col, when, year, month, dayofmonth, hour, row_number, lower, trim
from pyspark.sql.window import Window

def transform_transakcija_fact(
    db_trans_df, csv_trans_df,
    dim_kartica, dim_lokacija, dim_autentifikacija,
    dim_trgovac, dim_vrijeme, dim_kompozitna
):
    # Dedupliciranje dimenzija (uzimanje prvog retka ako ih ima više)
    w_kartica = Window.partitionBy("card_type", "card_age_category", "user_id").orderBy("card_tk")
    dim_kartica_dedup = dim_kartica.withColumn("rn", row_number().over(w_kartica)).filter(col("rn") == 1).drop("rn")

    w_lokacija = Window.partitionBy("city").orderBy("location_id")
    dim_lokacija_dedup = dim_lokacija.withColumn("rn", row_number().over(w_lokacija)).filter(col("rn") == 1).drop("rn")

    w_auth = Window.partitionBy("authentication_method").orderBy("authentication_id")
    dim_auth_dedup = dim_autentifikacija.withColumn("rn", row_number().over(w_auth)).filter(col("rn") == 1).drop("rn")

    w_trgovac = Window.partitionBy("merchant_category").orderBy("merchant_id")
    dim_trgovac_dedup = dim_trgovac.withColumn("rn", row_number().over(w_trgovac)).filter(col("rn") == 1).drop("rn")

    w_kompozitna = Window.partitionBy("ip_address_flag", "previous_fraudulent_activity").orderBy("kompozitna_tk")
    dim_kompozitna_dedup = dim_kompozitna.withColumn("rn", row_number().over(w_kompozitna)).filter(col("rn") == 1).drop("rn")

    w_vrijeme = Window.partitionBy("year", "month", "day", "hour").orderBy("vrijeme_tk")
    dim_vrijeme_dedup = dim_vrijeme.withColumn("rn", row_number().over(w_vrijeme)).filter(col("rn") == 1).drop("rn")

    # Baza podataka
    joined_db = (
        db_trans_df.alias("t")
        .join(dim_kartica.alias("k"), col("t.card_fk") == col("k.card_id"), "left")
        .join(dim_lokacija_dedup.alias("l"), col("t.location_fk") == col("l.location_id"), "left")
        .join(dim_auth_dedup.alias("a"), col("t.authentication_fk") == col("a.authentication_id"), "left")
        .join(dim_trgovac_dedup.alias("m"), col("t.merchant_fk") == col("m.merchant_tk"), "left")
        .join(dim_vrijeme_dedup.alias("d"),
              (year(col("t.timestamp")) == col("d.year")) &
              (month(col("t.timestamp")) == col("d.month")) &
              (dayofmonth(col("t.timestamp")) == col("d.day")) &
              (hour(col("t.timestamp")) == col("d.hour")), "left")
        .join(dim_kompozitna_dedup.alias("co"),
              (col("t.ip_address_flag") == col("co.ip_address_flag")) &
              (col("t.previous_fraudulent_activity") == col("co.previous_fraudulent_activity")), "left")
        .withColumn("account_balance_category", when(col("t.account_balance") < 1000, "Low")
                    .when((col("t.account_balance") >= 1000) & (col("t.account_balance") < 5000), "Average")
                    .otherwise("Above Average"))
        .select(
            col("t.id").alias("transaction_id"),
            col("t.transaction_amount"),
            col("t.daily_transaction_count"),
            col("t.avg_transaction_amount_7d"),
            col("t.failed_transaction_count_7d"),
            col("t.transaction_distance"),
            col("t.transaction_type"),
            col("t.fraud_label").cast("boolean").alias("fraud_label"),
            col("t.risk_score"),
            col("account_balance_category"),
            col("k.card_tk"),
            col("l.location_id"),
            col("a.authentication_id"),
            col("m.merchant_id"),
            col("d.vrijeme_tk"), 
            col("co.kompozitna_tk")
        )
    )

    # CSV podaci
    cleaned_csv = (
        csv_trans_df
        .withColumn("card_age_category", when(col("card_age") < 60, "New")
                    .when((col("card_age") >= 60) & (col("card_age") < 120), "Stable")
                    .otherwise("Long term"))
        .withColumn("account_balance_category", when(col("account_balance") < 1000, "Low")
                    .when((col("account_balance") >= 1000) & (col("account_balance") < 5000), "Average")
                    .otherwise("Above Average"))
    )

    # Transformacija podataka u CSV-u
    enriched = (
        cleaned_csv.alias("c")
        .join(dim_kartica_dedup.alias("k"),
              (col("c.card_type") == col("k.card_type")) & 
              (col("c.card_age_category") == col("k.card_age_category")) & 
              (col("c.user_id") == col("k.user_id")), "left")
        .join(dim_lokacija_dedup.alias("l"), col("c.location") == col("l.city"), "left")
        .join(dim_auth_dedup.alias("a"), trim(lower(col("c.authentication_method"))) == trim(lower(col("a.authentication_method"))), "left")
        .join(dim_trgovac_dedup.alias("m"), col("c.merchant_category") == col("m.merchant_category"), "left")
        .join(dim_vrijeme_dedup.alias("d"),
              (year(col("c.timestamp")) == col("d.year")) &
              (month(col("c.timestamp")) == col("d.month")) &
              (dayofmonth(col("c.timestamp")) == col("d.day")) &
              (hour(col("c.timestamp")) == col("d.hour")), "left")
        .join(dim_kompozitna_dedup.alias("co"),
              (col("c.ip_address_flag") == col("co.ip_address_flag")) & 
              (col("c.previous_fraudulent_activity") == col("co.previous_fraudulent_activity")), "left")
        .select(
            col("c.transaction_id"),
            col("c.transaction_amount"),
            col("c.daily_transaction_count"),
            col("c.avg_transaction_amount_7d"),
            col("c.failed_transaction_count_7d"),
            col("c.transaction_distance"),
            col("c.transaction_type"),
            col("c.fraud_label").cast("boolean").alias("fraud_label"),
            col("c.risk_score"),
            col("c.account_balance_category"),
            col("k.card_tk"),
            col("l.location_id"),
            col("a.authentication_id"),
            col("m.merchant_id"),
            col("d.vrijeme_tk"), 
            col("co.kompozitna_tk")
        )
    )

    # Union
    combined_df = joined_db.unionByName(enriched)

    fact_df = (
        combined_df
        .dropDuplicates(["transaction_id"]) 
        .withColumn("transaction_tk", row_number().over(Window.orderBy("transaction_id")))
        .select(
            "transaction_tk",
            "transaction_id",
            "transaction_amount",
            "daily_transaction_count",
            "avg_transaction_amount_7d",
            "failed_transaction_count_7d",
            "transaction_distance",
            "risk_score",
            "fraud_label",
            "account_balance_category",
            "transaction_type",
            col("vrijeme_tk").alias("vrijeme_id"),
            col("card_tk").alias("kartica_id"),
            col("location_id").alias("lokacija_id"),
            col("authentication_id").alias("autentifikacija_id"),
            col("merchant_id").alias("trgovac_id"),
            col("kompozitna_tk").alias("kompozitna_id"),
        )
    )

    # Provjera broja podataka
    assert fact_df.count() == 50000, "Number of transactions from step one of the project."

    return fact_df
