import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://root:root@localhost:3306/dw_dimensional')

query = """SELECT
    f.transaction_id,
    f.transaction_amount,
    f.daily_transaction_count,
    f.avg_transaction_amount_7d,
    f.failed_transaction_count_7d,
    f.transaction_distance,
    f.risk_score,
    f.fraud_label,
    f.account_balance_category,
    f.transaction_type,

    v.year,
    v.month,
    v.day,
    v.hour,
    v.is_weekend,

    k.card_type,
    k.card_age_category,
    k.date_from AS kartica_date_from,
    k.date_to AS kartica_date_to,
    k.user_id,

    l.city,
    l.population_category,
    l.state,

    a.authentication_method,
    a.authentication_method_category,
    a.date_from AS auth_date_from,
    a.date_to AS auth_date_to,

    t.merchant_category,
    t.merchant_type,

    c.ip_address_flag,
    c.previous_fraudulent_activity

FROM dw_dimensional.fact_transakcija f
LEFT JOIN dw_dimensional.dim_vrijeme v ON f.vrijeme_id = v.vrijeme_tk
LEFT JOIN dw_dimensional.dim_kartica k ON f.kartica_id = k.card_tk
LEFT JOIN dw_dimensional.dim_lokacija l ON f.lokacija_id = l.location_tk
LEFT JOIN dw_dimensional.dim_autentifikacija a ON f.autentifikacija_id = a.authentication_tk
LEFT JOIN dw_dimensional.dim_trgovac t ON f.trgovac_id = t.merchant_tk
LEFT JOIN dw_dimensional.dim_kompozitna c ON f.kompozitna_id = c.kompozitna_tk;
"""

df = pd.read_sql(query, engine)
df.to_csv('dataset.csv', index=False)

print("CSV sa svim podacima spremljen!")
