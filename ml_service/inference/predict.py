from pymongo import MongoClient
import pandas as pd
import numpy as np
import joblib

from ml_service.features.feature_engineering import create_features, FEATURE_COLUMNS


print("START PREDICT")

model = joblib.load("ml_service/model/logistic.pkl")
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

X_scaled = scaler.transform(X_latest)

pred = model.predict(X_scaled)[0]
prob = model.predict_proba(X_scaled)[0]

print("Signal:", "UP" if pred == 1 else "DOWN")
print("Confidence:", prob)