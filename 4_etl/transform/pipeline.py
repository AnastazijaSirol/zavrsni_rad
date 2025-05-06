from transform.dimensions.dim_kartica import transform_kartica_dim
from transform.dimensions.dim_lokacija import transform_lokacija_dim
from transform.dimensions.dim_autentifikacija import transform_autentifikacija_dim
from transform.dimensions.dim_trgovac import transform_trgovac_dim
from transform.dimensions.dim_vrijeme import transform_vrijeme_dim
from transform.dimensions.dim_kompozitna import transform_kompozitna_dim
from transform.facts.transakcija_fact import transform_transakcija_fact

def run_transformations(raw_data):
    
    dim_kartica = transform_kartica_dim(
    raw_data["card"],
    raw_data["user"],
    csv_card_df=raw_data.get("csv_transactions")
    )
    print("1️⃣ Card dimension complete")
    
    dim_lokacija = transform_lokacija_dim(
        raw_data["location"],
        raw_data["state"],
        csv_location_df=raw_data.get("csv_transaction")
    )
    print("2️⃣ Location dimension complete")

    dim_autentifikacija = transform_autentifikacija_dim(
        raw_data["authentication"],
        csv_authentication_df=raw_data.get("csv_transactions")
    )
    print("3️⃣ Authentication dimension complete")

    dim_trgovac = transform_trgovac_dim(
        raw_data["merchant"],
        csv_merchant_df=raw_data.get("csv_transactions")
    )
    print("4️⃣ Merchant dimension complete")

    dim_vrijeme = transform_vrijeme_dim(
        raw_data["transaction"],
        csv_date_df=raw_data.get("csv_transactions")
    )
    print("5️⃣ Time dimension complete")

    dim_kompozitna = transform_kompozitna_dim(
        raw_data["transaction"],
        csv_composite_df=raw_data.get("csv_transactions")
    )
    print("6️⃣ Composite dimension complete")

    fact_transakcija = transform_transakcija_fact(
        raw_data["transaction"],
        raw_data["csv_transactions"],
        dim_kartica,
        dim_lokacija,
        dim_autentifikacija,
        dim_trgovac,
        dim_vrijeme,
        dim_kompozitna
    )

    print("7️⃣ Transaction fact table complete")

    return {
        "dim_kartica": dim_kartica,
        "dim_lokacija": dim_lokacija,
        "dim_autentifikacija": dim_autentifikacija,
        "dim_trgovac": dim_trgovac,
        "dim_vrijeme": dim_vrijeme,
        "dim_kompozitna": dim_kompozitna,
        "fact_transakcija": fact_transakcija
    }