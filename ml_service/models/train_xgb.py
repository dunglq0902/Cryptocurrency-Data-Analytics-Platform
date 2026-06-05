import os
import joblib
import numpy as np
import pandas as pd

from pymongo import MongoClient
from sklearn.metrics import accuracy_score, classification_report
from xgboost import XGBClassifier

from ml_service.features.feature_engineering import create_features, FEATURE_COLUMNS


print("Connecting MongoDB...")
client = MongoClient("mongodb://root:changeme@localhost:27017/")
db = client["crypto_analytics"]

df = pd.DataFrame(list(db.gold_ohlcv.find({
    "symbol": "BTCUSDT",
    "timeframe": "5m"
})))

df = df.sort_values("window_start").reset_index(drop=True)
df = df.drop_duplicates(subset=["window_start"], keep="last")

df = df.replace([np.inf, -np.inf], np.nan)
df = df.dropna()

df["future_close"] = df["close"].shift(-5)
df["return_future"] = df["future_close"].pct_change()
df["target"] = (df["return_future"] > 0.002).astype(int)

df = create_features(df)
df = df.dropna()

X = df[FEATURE_COLUMNS]
y = df["target"]

split = int(len(df) * 0.8)

X_train = X.iloc[:split]
X_test = X.iloc[split:]
y_train = y.iloc[:split]
y_test = y.iloc[split:]

model = XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    eval_metric="logloss"
)

model.fit(X_train, y_train)

y_pred = model.predict(X_test)

print("Accuracy:", accuracy_score(y_test, y_pred))
print(classification_report(y_test, y_pred))

os.makedirs("ml_service/model", exist_ok=True)

joblib.dump(model, "ml_service/model/xgb.pkl")

print("Saved XGB")