# ML Service - Dự đoán xu hướng giá Crypto (UP / DOWN)

## 📌 Tổng quan

Module `ml_service` chịu trách nhiệm xây dựng hệ thống Machine Learning để dự đoán xu hướng giá ngắn hạn của crypto (BTCUSDT) dựa trên dữ liệu OHLCV lấy từ MongoDB.

Bài toán được định nghĩa là:
👉 **Phân loại nhị phân (Binary Classification)**

- `1` → Giá tăng (UP)
- `0` → Giá giảm (DOWN)

---

## ⚙️ Kiến trúc pipeline ML
MongoDB (dữ liệu OHLCV)
↓
Feature Engineering
↓
Tạo nhãn (Labeling)
↓
Train/Test theo thời gian (Time-series split)
↓
Huấn luyện model
↓
Đánh giá model
↓
Lưu model (.pkl)
↓
Inference (dự đoán)
↓
Ensemble (Voting)


---

## 📊 Feature Engineering

Các feature được xây dựng từ dữ liệu giá và khối lượng:

### 📌 Technical indicators:
- MA7, MA25 (Moving Average)
- RSI 14
- MACD & MACD Signal
- ATR 14
- VWAP

### 📌 Feature tự xây dựng:
- `return_1`: % thay đổi giá 1 bước
- `return_5`: % thay đổi giá 5 bước
- `trend`: MA7 - MA25
- `vol_ratio`: tỷ lệ volume so với trung bình
- `momentum_5`: độ tăng giá 5 bước
- `volatility_20`: độ biến động 20 bước

---

## 🤖 Các mô hình sử dụng

### 1. Logistic Regression
- Model baseline
- Cần chuẩn hóa dữ liệu (StandardScaler)
- Dễ giải thích

### 2. Random Forest
- Mô hình cây quyết định
- Tốt với dữ liệu phi tuyến

### 3. XGBoost
- Gradient boosting
- Thường cho kết quả tốt nhất trong 3 model

---

## 🧪 Ensemble (Kết hợp mô hình)

Kết quả cuối cùng được quyết định bằng voting:

- 3 model cùng dự đoán UP/DOWN
- Tổng hợp kết quả:
- 3 model cùng dự đoán UP/DOWN
- Tổng hợp kết quả:
    Nếu đa số là UP → BUY
    Nếu đa số là DOWN → SELL
    Nếu hòa → HOLD

---

## 📁 Cấu trúc thư mục

```bash
ml_service/
├── features/
│   └── feature_engineering.py      # Tạo feature chung
│
├── models/
│   ├── train_logistic.py
│   ├── train_rf.py
│   └── train_xgb.py
│
├── inference/
│   ├── predict.py                  # Dự đoán bằng 1 model
│   └── predict_ensemble.py         # Dự đoán bằng ensemble
│
├── model/
│   ├── logistic.pkl
│   ├── rf.pkl
│   ├── xgb.pkl
│   └── scaler_logistic.pkl
│
└── requirements.txt
```

--- 

## 🚀 Cách train model

Chạy lần lượt:

```bash
python -m ml_service.models.train_logistic
python -m ml_service.models.train_rf
python -m ml_service.models.train_xgb

```

---

## 🔮 Cách chạy dự đoán
1. Dự đoán bằng 1 model:
python -m ml_service.inference.predict
2. Dự đoán bằng ensemble:
python -m ml_service.inference.predict_ensemble

--- 

## 📈 Đánh giá mô hình
- Dữ liệu được chia theo thời gian (time-series split)
- Không shuffle dữ liệu để tránh leakage
- Metrics sử dụng:
    - Accuracy
    - Precision
    - Recall
    - F1-score

--- 

## ⚠️ Lưu ý quan trọng
- Đây là bài toán time-series classification, không phải classification thông thường
- Kết quả có thể thay đổi theo từng lần train do tính chất ngẫu nhiên của model
- XGBoost thường cho kết quả ổn định hơn
- Ensemble giúp giảm nhiễu và ổn định dự đoán

---

## 📌 Hướng phát triển tiếp theo (nếu mở rộng)
- Tối ưu hyperparameter (GridSearch / Optuna)
- Walk-forward validation
- Feature selection nâng cao
- Thêm Deep Learning (LSTM / Transformer)
- Build API realtime bằng FastAPI

---

### 👨‍💻 Ghi chú
Module này được xây dựng trong project hệ thống phân tích dữ liệu crypto end-to-end, bao gồm:
- Data ingestion
- Streaming (Spark)
- Storage (MongoDB)
- ML prediction

---
