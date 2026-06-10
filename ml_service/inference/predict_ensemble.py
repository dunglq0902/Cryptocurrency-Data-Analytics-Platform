from pymongo import MongoClient
import pandas as pd
import numpy as np
import joblib

from ml_service.features.feature_engineering import create_features, FEATURE_COLUMNS


print("Loading models...")

logistic = joblib.load("ml_service/model/logistic.pkl")
rf = joblib.load("ml_service/model/rf.pkl")
xgb = joblib.load("ml_service/model/xgb.pkl")
scaler = joblib.load("ml_service/model/scaler_logistic.pkl")


client = MongoClient("mongodb://root:changeme@localhost:27017/")
db = client["crypto_analytics"]

df = pd.DataFrame(list(db.gold_ohlcv.find({
    "symbol": "BTCUSDT",
    "timeframe": "5m"
})))

df = df.sort_values("window_start").reset_index(drop=True)

df = df.replace([np.inf, -np.inf], np.nan).dropna()

df = create_features(df)
df = df.dropna()

X = df[FEATURE_COLUMNS]
X_latest = X.iloc[[-1]]

p_log = logistic.predict(scaler.transform(X_latest))[0]
p_rf = rf.predict(X_latest)[0]
p_xgb = xgb.predict(X_latest)[0]

votes = [p_log, p_rf, p_xgb]

up = sum(votes)
down = len(votes) - up

if up >= 2:
    signal = "BUY"
elif down >= 2:
    signal = "SELL"
else:
    signal = "HOLD"

print("Logistic:", p_log)
print("RF:", p_rf)
print("XGB:", p_xgb)

print("BUY votes:", up)
print("SELL votes:", down)
print("FINAL SIGNAL:", signal)