import yfinance as yf
from datetime import date, timedelta
from typing import Union, Optional

GOLD = yf.Ticker("GC=F")

def _get_hist(start: str, end: str):
    df = GOLD.history(start=start, end=end, interval="1d") 
    if df.empty:
        raise ValueError(f"No data returned for {start} to {end}")
    return df

def get_open_close_by_date(date_str: str) -> str:
    next_day = (date.fromisoformat(date_str) + timedelta(days=1)).isoformat()
    df = _get_hist(date_str, next_day)
    o, c = df.loc[date_str, ["Open", "Close"]]
    return f"Gold on {date_str}: Open = {o:.2f} USD, Close = {c:.2f} USD"

def get_open_close_in_range(start_date: str, end_date: str) -> str:

    ex_end = (date.fromisoformat(end_date) + timedelta(days=1)).isoformat()
    df = _get_hist(start_date, ex_end)
    lines = [f"Gold prices from {start_date} to {end_date}:"]
    for dt, row in df.iterrows():
        lines.append(f"  • {dt.date()}: Open = {row['Open']:.2f}, Close = {row['Close']:.2f}")
    return "\n".join(lines)

def get_price_relative(days_ago: int = 0) -> str:

    target = date.today() - timedelta(days=days_ago)
    return get_open_close_by_date(target.isoformat())

def get_range_relative(
    days_ago: Optional[int] = 0
) -> str:

    start = date.today() - timedelta(days=days_ago)
    end = date.today()
    return get_open_close_in_range(start.isoformat(), end.isoformat())


print(get_open_close_by_date("2025-06-11"))
print(get_open_close_in_range("2025-06-01", "2025-06-07"))
print(get_price_relative(1))                             
print(get_range_relative(days_ago=2))
