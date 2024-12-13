from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

from db import engine, FactExchangeRate, DimensionCurrencyPair, DimensionTime

def extract(
    ticker_symbol: str,
    start_time: datetime=None,
    end_time: datetime=datetime.now()
):
    ticker = yf.Ticker(ticker_symbol)

    if start_time == None:
        start_time = end_time - timedelta(days=1)
    
    # extract only close rate
    hourly_data = ticker.history(interval="1h", start=start_time.strftime('%Y-%m-%d'), end=end_time.strftime('%Y-%m-%d'))[["Close"]]

    return hourly_data

def transform_rate(
    hourly_data: pd.DataFrame,
):
    transformed = []
    for i in range(len(hourly_data)):
        d = hourly_data.iloc[i]
        
        transformed.append({
            "time": {
                "hour": d.name.hour,
                "date": d.name.day,
                "month": d.name.month,
                "year": d.name.year
            },
            "rate": float(d.Close)
        })

    return transformed

def load_rate_and_time(transformed: list, id_currency_pair: int):
    # load the dimension time first
    times = [DimensionTime(
        hour=d["time"]["hour"],
        date=d["time"]["date"],
        month=d["time"]["month"],
        year=d["time"]["year"]
    ) for d in transformed]

    with Session(engine) as session:
        session.add_all(times)
        session.commit()
    
        for time in times:
            session.refresh(time)

    # load the fact rate
    rates = [FactExchangeRate(
        id_currency_pair=id_currency_pair,
        id_time=times[i].id_time,
        rate=d["rate"]
    ) for i, d in enumerate(transformed)]

    with Session(engine) as session:
        session.add_all(rates)
        session.commit()

if __name__ == "__main__":
    currency_map = {
        "USDIDR=X": ["usd", "idr"],
        "IDRUSD=X": ["idr", "usd"]
    }
    currency_ids = {}

    # get currency pair ids from dimension currency pair
    for key, currency in currency_map.items():
        with Session(engine) as session:
            currency_id = session.query(DimensionCurrencyPair.id_currency_pair).filter(
                DimensionCurrencyPair.from_currency == currency[0],
                DimensionCurrencyPair.to_currency == currency[1]
            ).one()[0]

            currency_ids[key] = currency_id
    
    # etl for each currency pair
    for key in currency_map.keys():
        hourly_data = extract(key)
        transformed = transform_rate(hourly_data)
        load_rate_and_time(transformed, currency_ids[key])