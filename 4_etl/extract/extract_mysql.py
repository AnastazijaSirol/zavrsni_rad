from spark_session import get_spark_session

from spark_session import get_spark_session

def extract_table(table_name):
    spark = get_spark_session("ETL_App")

    jdbc_url = "jdbc:mysql://localhost:3306/dw?allowPublicKeyRetrieval=true&useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    if table_name == "transaction":
        query = """
            SELECT 
                CAST(timestamp AS CHAR(30)) AS timestamp,
                id, 
                transaction_amount, 
                transaction_type, 
                ip_address_flag, 
                transaction_distance, 
                risk_score, 
                is_weekend, 
                fraud_label, 
                account_balance, 
                daily_transaction_count, 
                avg_transaction_amount_7d, 
                failed_transaction_count_7d, 
                previous_fraudulent_activity,
                card_fk,
                device_fk,
                location_fk,
                authentication_fk,
                merchant_fk
            FROM transaction
        """
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({query}) AS transaction_alias",
            properties=connection_properties
        )
    else:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_properties
        )

    return df

def extract_all_tables():
    return {
        "card": extract_table("card"),
        "user": extract_table("user"),
        "location": extract_table("location"),
        "state": extract_table("state"),
        "authentication": extract_table("authentication"),
        "merchant": extract_table("merchant"),
        "device": extract_table("device"),
        "transaction": extract_table("transaction"),
    }