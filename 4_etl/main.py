from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from spark_session import get_spark_session
from load.run_loading import write_spark_df_to_mysql
import os

os.environ.pop("SPARK_HOME", None)

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    # Load
    print("🚀 Starting data extraction")
    mysql_df = extract_all_tables()
    csv_df = {"csv_transactions":extract_from_csv("C:/Users/Pc/Desktop/6_semestar/SRP/Koraci/Checkpoint_2/synthetic_fraud_dataset_PROCESSED_20.csv")}
    merged_df = {**mysql_df, **csv_df}
    print("✅ Data extraction completed")

    # Transform
    print("🚀 Starting data transformation")
    load_ready_dict = run_transformations(merged_df)
    print("✅ Data transformation completed")

    # Load
    print("🚀 Starting data loading")
    for table_name, df in load_ready_dict.items():
        write_spark_df_to_mysql(df, table_name)
    print("👏 Data loading completed")

if __name__ == "__main__":
    main()