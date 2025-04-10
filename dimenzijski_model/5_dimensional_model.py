from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, DECIMAL, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/dw_dimensional"
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# DIMENZIJE

class DimKompozitna(Base):
    __tablename__ = 'dim_kompozitna'
    __table_args__ = {'schema': 'dw_dimensional'}

    kompozitna_tk = Column(Integer, primary_key=True, autoincrement=True)
    ip_address_flag = Column(Boolean, nullable=False)
    previous_fraudulent_activity = Column(Boolean, nullable=False)

class DimVrijeme(Base):
    __tablename__ = 'dim_vrijeme'
    __table_args__ = {'schema': 'dw_dimensional'}

    vrijeme_tk = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    hour = Column(Integer, nullable=False)
    is_weekend = Column(Boolean, nullable=False)

class DimKartica(Base):
    __tablename__ = 'dim_kartica'
    __table_args__ = {'schema': 'dw_dimensional'}

    card_tk = Column(Integer, primary_key=True, autoincrement=True)
    card_id = Column(String(50), index=True)
    card_type = Column(String(50), nullable=False)
    card_age_category = Column(String(50), nullable=False)
    user_id = Column(String(50), nullable=False)

    # Sporo mijenjajuće dimenzije
    date_from = Column(DateTime, nullable=False)
    date_to = Column(DateTime, nullable=True)

class DimLokacija(Base):
    __tablename__ = 'dim_lokacija'
    __table_args__ = {'schema': 'dw_dimensional'}

    location_tk = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, index=True)
    city = Column(String(100), nullable=False)
    population_category = Column(String(50), nullable=False)
    state = Column(String(100), nullable=False)

class DimAutentifikacija(Base):
    __tablename__ = 'dim_autentifikacija'
    __table_args__ = {'schema': 'dw_dimensional'}

    authentication_tk = Column(Integer, primary_key=True, autoincrement=True)
    authentication_id = Column(Integer, index=True)
    authentication_method = Column(String(100), nullable=False)
    authentication_method_category = Column(String(50), nullable=False)

    # Sporo mijenjajuće dimenzije
    date_from = Column(DateTime, nullable=False)
    date_to = Column(DateTime, nullable=True)

class DimTrgovac(Base):
    __tablename__ = 'dim_trgovac'
    __table_args__ = {'schema': 'dw_dimensional'}

    merchant_tk = Column(Integer, primary_key=True, autoincrement=True)
    merchant_id = Column(Integer, index=True)
    merchant_category = Column(String(100), nullable=False)
    merchant_type = Column(String(50), nullable=False)

# TABLICA ČINJENICA

class FactTransakcija(Base):
    __tablename__ = 'fact_transakcija'
    __table_args__ = {'schema': 'dw_dimensional'}

    transaction_tk = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(50), index=True)
    transaction_amount = Column(DECIMAL(20,2), nullable=False)
    transaction_distance = Column(DECIMAL(20,2), nullable=False)
    risk_score = Column(Float, nullable=False)
    fraud_label = Column(Boolean, nullable=False)

    # Degenerirane dimenzije
    account_balance_category = Column(String(50), nullable=False)
    transaction_type = Column(String(50), nullable=False)

    vrijeme_id = Column(Integer, ForeignKey('dw_dimensional.dim_vrijeme.vrijeme_tk'))
    kartica_id = Column(Integer, ForeignKey('dw_dimensional.dim_kartica.card_tk'))
    lokacija_id = Column(Integer, ForeignKey('dw_dimensional.dim_lokacija.location_tk'))
    autentifikacija_id = Column(Integer, ForeignKey('dw_dimensional.dim_autentifikacija.authentication_tk'))
    trgovac_id = Column(Integer, ForeignKey('dw_dimensional.dim_trgovac.merchant_tk'))
    kompozitna_id = Column(Integer, ForeignKey('dw_dimensional.dim_kompozitna.kompozitna_tk'))

Base.metadata.create_all(engine)
print("Dimenzijski model (star schema) kreiran uspješno.")
