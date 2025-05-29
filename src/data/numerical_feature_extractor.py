import pandas as pd
import numpy as np
import yaml
from ta.trend import EMAIndicator, SMAIndicator, MACD
from ta.momentum import RSIIndicator
from datetime import datetime


def load_config(path: str) -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def load_ohlcv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    breakpoint()
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    return df


def calculate_returns(df: pd.DataFrame, log=True, simple=True):
    if log:
        df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    if simple:
        df['simple_return'] = df['close'].pct_change()
    return df


def add_sma(df: pd.DataFrame, periods):
    for p in periods:
        df[f'sma_{p}'] = SMAIndicator(close=df['close'], window=p).sma_indicator()
    return df


def add_ema(df: pd.DataFrame, periods):
    for p in periods:
        df[f'ema_{p}'] = EMAIndicator(close=df['close'], window=p).ema_indicator()
    return df


def add_macd(df: pd.DataFrame):
    macd = MACD(close=df['close'])
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    df['macd_diff'] = macd.macd_diff()
    return df


def add_rsi(df: pd.DataFrame, period):
    rsi = RSIIndicator(close=df['close'], window=period)
    df['rsi'] = rsi.rsi()
    return df


def filter_by_date(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    return df.loc[start_date:end_date]


def main():
    config = load_config("../../configs/numerical_feature_extractor.yaml")

    raw_data_path = config["paths"]["raw_data"]
    processed_data_path = config["paths"]["processed_data"]

    df = load_ohlcv(raw_data_path)
    df = filter_by_date(df, config['start_date'], config['end_date'])

    features = config.get('features', {})

    df = calculate_returns(
        df, 
        log=features.get('log_return', False),
        simple=features.get('simple_return', False)
    )

    if 'sma' in features:
        df = add_sma(df, features['sma'].get('periods', []))

    if 'ema' in features:
        df = add_ema(df, features['ema'].get('periods', []))

    if features.get('macd', False):
        df = add_macd(df)

    if 'rsi' in features:
        df = add_rsi(df, features['rsi'].get('period', 14))

    df.dropna(inplace=True)
    df.to_csv(processed_data_path)

    print(f"Features saved to: {processed_data_path}")


if __name__ == "__main__":
    main()
