import numpy as np

def create_features(df):
    df = df.copy()

    df["return_1"] = df["close"].pct_change(1).shift(1)
    df["return_5"] = df["close"].pct_change(5).shift(1)

    df["trend"] = (df["ma7"] - df["ma25"]).shift(1)

    df["vol_ratio"] = (df["volume"] / df["volume"].rolling(10).mean()).shift(1)

    df["momentum_5"] = (df["close"] - df["close"].shift(5)).shift(1)

    df["volatility_20"] = df["close"].rolling(20).std().shift(1)
    
    return df


FEATURE_COLUMNS = [
    "ma7","ma25","rsi_14","macd","macd_signal",
    "atr_14","volume_ratio","volume","trade_count","vwap",
    "return_1","return_5","trend",
    "vol_ratio","momentum_5","volatility_20"
]