import os

from sqlalchemy import create_engine, MetaData, Column, Integer, Float, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship, Session

base_dir = os.path.abspath(os.path.dirname(__file__))
db_path = os.path.join(base_dir, "data.sqlite")

engine = create_engine(f"sqlite:///{db_path}")
meta = MetaData()

class Base(DeclarativeBase):
    pass

class DimensionTime(Base):
    __tablename__ = "dimension_time"

    id_time = Column("id_time", Integer, primary_key=True, autoincrement=True)
    hour = Column("hour", Integer)
    date = Column("date", Integer)
    month = Column("month", Integer)
    year = Column("year", Integer)

    fact_exchange_rate = relationship("FactExchangeRate", back_populates="dimension_time")

class DimensionCurrencyPair(Base):
    __tablename__ = "dimension_currency_pair"

    id_currency_pair = Column("id_currency_pair", Integer, primary_key=True)
    from_currency = Column("from_currency", String)
    to_currency = Column("to_currency", String)

    fact_exchange_rate = relationship("FactExchangeRate", back_populates="dimension_currency_pair")

class FactExchangeRate(Base):
    __tablename__ = "fact_exchange_rate"

    id_rate = Column("id_rate", Integer, primary_key=True, autoincrement=True)
    id_time = Column(Integer, ForeignKey("dimension_time.id_time"))
    id_currency_pair = Column(Integer, ForeignKey("dimension_currency_pair.id_currency_pair"))
    rate = Column("rate", Float)

    dimension_time = relationship("DimensionTime", back_populates="fact_exchange_rate")
    dimension_currency_pair = relationship("DimensionCurrencyPair", back_populates="fact_exchange_rate")

Base.metadata.create_all(engine)

# initialize currency pair
with Session(engine) as session:
    if not session.query(DimensionCurrencyPair).first():
        usd_to_idr = DimensionCurrencyPair(
            id_currency_pair=0,
            from_currency="usd",
            to_currency="idr"
        )

        idr_to_usd = DimensionCurrencyPair(
            id_currency_pair=1,
            from_currency="idr",
            to_currency="usd"
        )

        session.add_all([
            usd_to_idr,
            idr_to_usd
        ])

        session.commit()