import pandas as pd
import numpy as np
import yaml
from ta.trend import (
    EMAIndicator, SMAIndicator, MACD, ADXIndicator, CCIIndicator, VortexIndicator
)
from ta.momentum import (
    RSIIndicator, StochasticOscillator, WilliamsRIndicator, ROCIndicator
)
from ta.volatility import BollingerBands, AverageTrueRange, DonchianChannel
from ta.volume import OnBalanceVolumeIndicator
from datetime import datetime


def load_config(path: str) -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def load_ohlcv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
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


def add_stochastic(df: pd.DataFrame, k_window, d_window):
    stoch = StochasticOscillator(high=df['high'], low=df['low'], close=df['close'], window=k_window, smooth_window=d_window)
    df['stoch_k'] = stoch.stoch()
    df['stoch_d'] = stoch.stoch_signal()
    return df


def add_williams_r(df: pd.DataFrame, period):
    willr = WilliamsRIndicator(high=df['high'], low=df['low'], close=df['close'], lbp=period)
    df['williams_r'] = willr.williams_r()
    return df


def add_cci(df: pd.DataFrame, period):
    cci = CCIIndicator(high=df['high'], low=df['low'], close=df['close'], window=period)
    df['cci'] = cci.cci()
    return df


def add_roc(df: pd.DataFrame, period):
    roc = ROCIndicator(close=df['close'], window=period)
    df['roc'] = roc.roc()
    return df


def add_atr(df: pd.DataFrame, period):
    atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=period)
    df['atr'] = atr.average_true_range()
    return df


def add_bollinger_bands(df: pd.DataFrame, period):
    bb = BollingerBands(close=df['close'], window=period, window_dev=2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_middle'] = bb.bollinger_mavg()
    df['bb_lower'] = bb.bollinger_lband()
    return df


def add_donchian(df: pd.DataFrame, period):
    dc = DonchianChannel(high=df['high'], low=df['low'], close=df['close'], window=period)
    df['donchian_upper'] = dc.donchian_channel_hband()
    df['donchian_lower'] = dc.donchian_channel_lband()
    return df


def add_adx(df: pd.DataFrame, period):
    adx = ADXIndicator(high=df['high'], low=df['low'], close=df['close'], window=period)
    df['adx'] = adx.adx()
    return df


def add_vortex(df: pd.DataFrame, period):
    vortex = VortexIndicator(high=df['high'], low=df['low'], close=df['close'], window=period)
    df['vortex_pos'] = vortex.vortex_indicator_pos()
    df['vortex_neg'] = vortex.vortex_indicator_neg()
    return df


def add_obv(df: pd.DataFrame):
    obv = OnBalanceVolumeIndicator(close=df['close'], volume=df['volume'])
    df['obv'] = obv.on_balance_volume()
    return df


def add_time_features(df: pd.DataFrame):
    df['hour_sin'] = np.sin(2 * np.pi * df.index.hour / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df.index.hour / 24)
    df['weekday_sin'] = np.sin(2 * np.pi * df.index.dayofweek / 7)
    df['weekday_cos'] = np.cos(2 * np.pi * df.index.dayofweek / 7)
    return df

def label_by_open_close(df: pd.DataFrame, threshold: float = 0.001) -> pd.DataFrame:
    """
    Labels:
        2 → Buy (close significantly > open)
        0 → Sell (close significantly < open)
        1 → Neutral
    """
    delta = (df['close'] - df['open']) / df['open']
    df['label'] = np.select(
        [delta > threshold, delta < -threshold],
        [1, 0],  # 1: Buy, 0: Sell
        default=2  # 1: Neutral
    )
    return df



def main():
    config = load_config("configs/numerical_feature_extractor.yaml")

    df = load_ohlcv(config['paths']['raw_data'])
    df = df.loc[config['start_date']:config['end_date']]

    features = config.get('features', {})

    df = calculate_returns(df, log=features.get('log_return', False), simple=features.get('simple_return', False))

    if 'sma' in features:
        df = add_sma(df, features['sma'].get('periods', []))
    if 'ema' in features:
        df = add_ema(df, features['ema'].get('periods', []))
    if features.get('macd', False):
        df = add_macd(df)
    if 'rsi' in features:
        df = add_rsi(df, features['rsi'].get('period', 14))
    if 'stochastic' in features:
        df = add_stochastic(df, features['stochastic']['k'], features['stochastic']['d'])
    if 'williams_r' in features:
        df = add_williams_r(df, features['williams_r'])
    if 'cci' in features:
        df = add_cci(df, features['cci'])
    if 'roc' in features:
        df = add_roc(df, features['roc'])
    if 'atr' in features:
        df = add_atr(df, features['atr'])
    if 'bollinger_bands' in features:
        df = add_bollinger_bands(df, features['bollinger_bands'])
    if 'donchian' in features:
        df = add_donchian(df, features['donchian'])
    if 'adx' in features:
        df = add_adx(df, features['adx'])
    if 'vortex' in features:
        df = add_vortex(df, features['vortex'])
    if features.get('obv', False):
        df = add_obv(df)
    if features.get('time_features', False):
        df = add_time_features(df)

    df.dropna(inplace=True)

    # ➕ Add label column
    threshold = config.get('labeling', {}).get('threshold', 0.001)
    df = label_by_open_close(df, threshold)

    df.to_csv(config['paths']['processed_data'])
    print(f"Features saved to: {config['paths']['processed_data']}")

main()