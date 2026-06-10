# Cryptocurrency Data Analytics Platform

> **Streaming Lakehouse Architecture | Real-time Trading Signals**  
> Big Data Systems — Optimized for Kubernetes Deployment
---
## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Kiến trúc hệ thống](#2-kiến-trúc-hệ-thống)
3. [Cấu trúc thư mục](#3-cấu-trúc-thư-mục)
4. [Stack công nghệ](#4-stack-công-nghệ)
5. [Yêu cầu môi trường](#5-yêu-cầu-môi-trường)
6. [Quickstart – Kubernetes Deployment](#6-quickstart--kubernetes-deployment)
7. [Cấu hình chi tiết](#7-cấu-hình-chi-tiết)
8. [Unified Spark Streaming Job](#8-unified-spark-streaming-job)
9. [Alert Engine](#9-alert-engine)
10. [Machine Learning](#10-machine-learning)
11. [Tests](#11-tests)
12. [CI/CD](#12-cicd)

---

## 1. Tổng quan

**Cryptocurrency Data Analytics Platform** là hệ thống xử lý dữ liệu lớn toàn diện, thu thập và phân tích dữ liệu thị trường tiền mã hóa theo thời gian thực từ sàn giao dịch Binance. Hệ thống áp dụng kiến trúc **Streaming Lakehouse** kết hợp với **Medallion Architecture** (Bronze → Silver → Gold) và cung cấp hệ thống cảnh báo tín hiệu giao dịch (mua/bán) có khả năng cá nhân hóa cao.

### Kiến trúc tối ưu hóa

Hệ thống được thiết kế linh hoạt và tối ưu hóa tài nguyên:
- **Kafka KRaft mode**
- **Unified Spark Streaming** — gộp 4 jobs thành 1 `foreachBatch` pipeline
- **APScheduler**
- **Monitoring**

### Tính năng chính

| Tính năng | Mô tả |
|---|---|
| **Real-time Ingestion** | WebSocket streaming từ Binance (tick data, OHLCV, order book) |
| **Batch Backfill** | REST API producer để nạp dữ liệu lịch sử (APScheduler) |
| **Unified Streaming ETL** | 1 Spark job: Bronze → Silver → Gold → Alerts |
| **Technical Indicators** | RSI, MACD, Bollinger Bands, MA, ATR, Volume Profile |
| **Alert Engine** | Hệ thống cảnh báo mua/bán với điều kiện lọc tùy chỉnh |
| **Notifications** | Telegram Bot, Email (SMTP), Webhook |
| **Dashboard UI** | Streamlit Web App để quản lý rules và visualize dữ liệu |

---

## 2. Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                              │
│   Binance WebSocket ──► Kafka (raw-ohlcv, raw-ticks, raw-orderbook) │
│   APScheduler (daily backfill, hourly data quality check)           │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ Spark Structured Streaming
                              │ (Unified foreachBatch Pipeline)
┌─────────────────────────────▼───────────────────────────────────────┐
│                   PROCESSING LAYER (1 Spark Driver)                  │
│  Bronze (raw)  ──►  Silver (cleaned + indicators)  ──►  Gold (agg) │
│  MinIO/Delta       MinIO/Delta Lake                  MinIO/Delta    │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                   ┌──────────┴──────────┐
                   ▼                     ▼
          ┌──────────────────┐    ┌────────────────────┐
          │ Alert Consumer   │    │   Delta Lake        │
          │ (Kafka Bridge)   │    │   Query / Analytics │
          └────────┬─────────┘    │   (SQL / Spark)     │
                   │              └────────────────────┘
                   ▼
          ┌──────────────────┐    ┌────────────────────┐
          │  Alert Engine    │◄───┤ Dashboard (UI)      │
          │  FastAPI + Rules │    │ (Streamlit)         │
          │  (MongoDB)       │    └────────────────────┘
          └────────┬─────────┘
                   │
      ┌────────────┴────────────┐
      ▼                         ▼
Telegram Bot            Email / Webhook
```

### Medallion Architecture

| Zone | Description | Format |
|---|---|---|
| **Bronze** | Raw data từ Kafka, không biến đổi, có metadata | Delta Lake (MinIO) |
| **Silver** | Đã làm sạch, normalize, technical indicators | Delta Lake (MinIO) |
| **Gold** | Multi-timeframe OHLCV, VWAP, window functions | Delta Lake (MinIO) |

---

## 3. Cấu trúc thư mục

```
crypto-analytics-platform/
│
├── ingestion/                      # Binance API producers
│   ├── main.py                    # Entry point (WebSocket + APScheduler)
│   ├── binance_ws_producer.py     # WebSocket streaming (tick data)
│   ├── binance_rest_producer.py   # REST API (historical OHLCV)
│   ├── kafka_config.py            # Kafka settings, topic definitions
│   ├── scheduler.py               # APScheduler (thay Airflow)
│   └── startup_backfill.py        # Tự động nạp dữ liệu quá khứ khi khởi động
│
├── spark/                          # PySpark jobs & utilities
│   ├── jobs/
│   │   └── unified_streaming.py   # Unified pipeline (Bronze→Silver→Gold→Alert)
│   ├── udfs/
│   │   └── indicator_udfs.py      # RSI, MACD, candle classifier UDFs
│   ├── schemas/
│   │   └── bronze_schema.py       # Spark DataFrame schemas (Bronze/Silver/Gold)
│   └── utils/
│       └── spark_session.py       # SparkSession factory + helpers
│
├── alert_engine/                   # Alert Engine microservice
│   ├── api/
│   │   ├── main.py                # FastAPI app (CRUD + dispatch endpoint)
│   │   ├── models.py              # Pydantic models for rules & events
│   │   └── routes/
│   │       └── rules.py           # /api/v1/rules CRUD router
│   ├── consumer/
│   │   └── alert_consumer.py      # Kafka consumer nhận events báo động
│   ├── evaluator/
│   │   └── rule_engine.py         # Pure-Python rule condition evaluator
│   └── notifier/
│       ├── notifiers.py           # Telegram, Email, Webhook dispatchers
│       └── notification_service.py # FastAPI notification microservice
│
├── dashboard/                      # Streamlit UI (UI cho chạy K8s)
│   ├── app.py                     # Quản lý Alert Rules & Monitoring
│   └── requirements.txt
│
├── monitoring/                     # Cấu hình Prometheus & Grafana
│   ├── prometheus/
│   └── grafana/
│
├── docker/                         # Dockerfiles & requirements
│   ├── Dockerfile.spark
│   ├── Dockerfile.alert-engine
│   ├── Dockerfile.ingestion
│   ├── Dockerfile.dashboard
│   └── ...
│
├── scripts/
│   ├── setup.sh                   # One-shot local dev setup
│   ├── teardown.sh                # Stop and optionally remove volumes
│   ├── seed_data.py               # Seed MongoDB with sample alert rules
│   ├── submit_spark_job.sh        # spark-submit helper
│   ├── produce_test_msgs.py       # Produce test Kafka messages
│   ├── run_bronze_smoke.py        # Smoke test for streaming pipeline
│   ├── backfill.py                # Trigger backfill thủ công
│   └── create_secrets.sh          # Khởi tạo k8s secrets
│
├── tests/                          # Unit & integration tests
│   ├── ...                        # Các file tests
│
├── k8s/                            # Kubernetes manifests
├── helm/                           # Helm charts
├── .env.example                    # Environment variable template
├── requirements.txt                # Aggregated Python dependencies
└── README.md
```

---

## 4. Stack công nghệ

| Thành phần | Công nghệ | Phiên bản |
|---|---|---|
| Data Source | Binance WebSocket/REST API | v3 |
| Message Queue | Apache Kafka (KRaft mode) | 7.7.1 |
| Stream Processing | Apache Spark (PySpark) | 3.5.2 |
| Storage | MinIO (S3-compatible) + Delta Lake | 3.2.0 |
| NoSQL Database | MongoDB | 7.0 |
| Scheduling | APScheduler (thay Airflow) | 3.10.4 |
| Alert Engine | FastAPI + Motor (async MongoDB) | 0.111.0 / 3.4.0 |
| Dashboard | Streamlit | Mới nhất |
| Containerization | Docker | 24+ |
| K8s Distribution | Kubernetes (k3s, minikube, EKS, v.v.) | 1.28+ |
| Language | Python | 3.11+ |

---

## 5. Yêu cầu môi trường

### Kubernetes Cluster

| Công cụ | Phiên bản tối thiểu |
|---|---|
| Kubernetes (k3s, minikube, EKS, AKS, v.v.) | 1.28+ |
| Helm | 3.13+ |
| kubectl | 1.28+ |

---

## 6. Quickstart – Kubernetes Deployment

Dự án được thiết kế để chạy trực tiếp trên Kubernetes. Bạn có thể sử dụng các raw YAML manifests trong thư mục `k8s/` hoặc Helm charts trong `helm/`.

### Bước 1: Clone và cấu hình

```bash
git clone https://github.com/your-org/crypto-analytics-platform.git
cd crypto-analytics-platform

# Tạo file .env từ template
cp .env.example .env
# Điền Binance API keys và Telegram bot token vào .env
```

### Bước 2: Deploy lên Kubernetes

Dành cho môi trường đã cài đặt Kubernetes (có thể dùng script `deploy.ps1` nếu dùng Windows với Docker Desktop/Kind):

```powershell
# Chạy script để tự động build images và deploy tất cả K8s resources
.\k8s\deploy.ps1
```
*(Chi tiết các bước deploy và yaml có sẵn trong thư mục `k8s/`)*

Hoặc triển khai thủ công bằng kubectl:
```bash
kubectl apply -f k8s/
```

### Bước 3: Kiểm tra trạng thái

```bash
kubectl get pods -n crypto-analytics
kubectl logs -l app=spark-unified-job -n crypto-analytics
```

### Bước 4: Truy cập các giao diện (Port Forwarding)

Sử dụng `kubectl port-forward` để truy cập các dịch vụ (ví dụ):

| Service | Port Forwarding Command | URL |
|---|---|---|
| Dashboard | `kubectl port-forward svc/dashboard 8501:8501 -n crypto-analytics` | http://localhost:8501 |
| Alert API | `kubectl port-forward svc/alert-api 8000:8000 -n crypto-analytics` | http://localhost:8000/docs |

---

## 7. Cấu hình chi tiết

### Environment Variables quan trọng

| Biến | Mô tả | Giá trị mặc định |
|---|---|---|
| `BINANCE_API_KEY` | Binance API key | — |
| `BINANCE_API_SECRET` | Binance API secret | — |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:9092` |
| `S3_ENDPOINT` | MinIO S3 Endpoint | `http://minio:9000` |
| `MONGO_URI` | MongoDB connection string | `mongodb://root:changeme@localhost:27017` |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API token | — |
| `SPARK_MASTER` | Spark master URL | `spark://localhost:7077` |

### Kafka Topics

| Topic | Partitions | Retention | Mô tả |
|---|---|---|---|
| `raw-crypto-ticks` | 5 | 7 ngày | Individual trade events |
| `raw-ohlcv` | 5 | 7 ngày | 1-min OHLCV candles |
| `raw-orderbook` | 5 | 7 ngày | Best bid/ask |
| `processed-signals` | 5 | 30 ngày | Processed trading signals |
| `alert-events` | 5 | 30 ngày | Alert trigger events |

---

## 8. Unified Spark Streaming Job

### Kiến trúc: 1 Driver thay vì 4

Thay vì 4 Spark jobs riêng biệt (Bronze, Silver, Gold, Alert), hệ thống gộp tất cả vào 1 `foreachBatch` pipeline trong `spark/jobs/unified_streaming.py`:

```python
def process_batch(batch_df, batch_id):
    bronze_df = transform_bronze(batch_df)      # Parse Kafka JSON, validate
    silver_df = transform_silver(bronze_df)      # Clean, dedup, indicators
    gold_df   = transform_gold(silver_df)        # Multi-timeframe resample
    alerts    = evaluate_alerts(gold_df)          # Rule evaluation vs MongoDB

    bronze_df.write.format("delta").save(...)
    silver_df.write.format("delta").save(...)
    gold_df.write.format("delta").save(...)
    alerts.write.format("kafka").save(...)        # → alert-events topic
```

### Chạy thủ công

```bash
bash scripts/submit_spark_job.sh unified
```

### Technical Indicators được tính

| Indicator | Mô tả | Window |
|---|---|---|
| MA7, MA25, MA99 | Moving Averages | 7, 25, 99 nến |
| Bollinger Bands | Upper/Middle/Lower band | 20 nến, 2σ |
| RSI(14) | Relative Strength Index | 14 nến |
| MACD | 12-26-9 | SMA-based |
| ATR(14) | Average True Range | 14 nến |
| Volume Ratio | Volume / MA(20) volume | 20 nến |
| Candle Pattern | Doji, Hammer, Shooting Star, etc. | Per-candle |
| VWAP | Volume-Weighted Average Price | Gold layer |

### Multi-Timeframe Resampling (Gold Layer)

| Timeframe | Duration |
|---|---|
| 5m | 5 minutes |
| 15m | 15 minutes |
| 1h | 1 hour |
| 4h | 4 hours |
| 1d | 1 day |

---

## 9. Alert Engine

### API Endpoints

```
POST   /api/v1/rules/              # Tạo rule mới
GET    /api/v1/rules/              # Liệt kê rules (có phân trang)
GET    /api/v1/rules/{rule_id}     # Lấy chi tiết rule
PATCH  /api/v1/rules/{rule_id}     # Cập nhật một phần rule
DELETE /api/v1/rules/{rule_id}     # Xóa rule
POST   /api/v1/rules/{rule_id}/toggle  # Bật/tắt rule
GET    /api/v1/rules/{rule_id}/history # Lịch sử trigger của rule

POST   /api/v1/notifications/dispatch  # Dispatch alert event (internal)
GET    /health                         # Health check
```

### Ví dụ tạo Alert Rule

```bash
curl -X POST http://localhost:8000/api/v1/rules/ \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-001" \
  -d '{
    "symbol": "BTCUSDT",
    "timeframe": "1h",
    "logic": "AND",
    "action": "BUY",
    "conditions": [
      {"field": "rsi_14",         "operator": "<",  "value": 30},
      {"field": "volume_ratio",   "operator": ">",  "value": 1.5},
      {"field": "candle_pattern", "operator": "==", "value": "HAMMER"}
    ],
    "notification_channels": ["email"],
    "cooldown_seconds": 300
  }'
```

### Các điều kiện lọc hỗ trợ

| Field | Operators | Ví dụ |
|---|---|---|
| `close`, `open`, `high`, `low` | `>`, `<`, `>=`, `<=`, `crosses_above`, `crosses_below` | `close > 50000` |
| `volume`, `volume_ratio` | `>`, `<`, `>=`, `<=` | `volume_ratio > 2.0` |
| `rsi_14` | `>`, `<`, `>=`, `<=` | `rsi_14 < 30` |
| `macd`, `macd_signal` | tất cả operators | `macd crosses_above 0` |
| `ma7`, `ma25`, `ma99` | tất cả operators | `ma7 > ma25` |
| `candle_pattern` | `==`, `!=` | `candle_pattern == HAMMER` |
| `vwap` | `>`, `<`, `>=`, `<=` | `close > vwap` |

### Notification Channels

| Channel | Cấu hình |
|---|---|
| **Email** | Set `SMTP_*` env vars + `email_address` in rule |
---

## 10. Machine Learning

*Lưu ý: Tính năng Machine Learning đang trong quá trình phát triển. Mã nguồn sẽ được bổ sung trong bản cập nhật tới.*

Hệ thống được thiết kế để tích hợp các mô hình Machine Learning nhằm nâng cao khả năng phân tích và dự đoán, dự kiến bao gồm:
- **Price Prediction**: Dự đoán giá ngắn hạn bằng các mô hình Time-series (LSTM, Prophet) dựa trên dữ liệu OHLCV.
- **Sentiment Analysis**: Phân tích cảm xúc thị trường thông qua dữ liệu thu thập từ các nguồn tin tức.
- **Anomaly Detection**: Phát hiện các bất thường trong giao dịch.
- **Model Training Pipeline**: Quy trình huấn luyện mô hình tự động chạy định kỳ bằng dữ liệu lịch sử.

---

## 11. Tests

```bash
# Cài đặt test dependencies
pip install pytest pytest-asyncio httpx

# Chạy tất cả unit tests
pytest tests/ -v --ignore=tests/test_spark_jobs.py

# Chạy Spark integration tests (cần PySpark)
pytest tests/test_spark_jobs.py -m spark -v

# Chạy với coverage
pytest tests/ --cov=. --cov-report=html

# Chạy test cụ thể
pytest tests/test_alert_engine.py::TestRuleEvaluator -v
pytest tests/test_ingestion.py::TestKlineParser -v
```

### Test coverage

| Module | Tests |
|---|---|
| `ingestion/` | `test_ingestion.py` – parsers, producers, partition logic |
| `alert_engine/evaluator/` | `test_alert_engine.py` – conditions, AND/OR logic, batch eval |
| `alert_engine/api/` | `test_alert_api.py` – CRUD endpoints, validation |
| `spark/udfs/` | `test_spark_udfs.py` – candle classifier, RSI, formatters |
| `spark/jobs/` | `test_spark_jobs.py` – Silver clean, Gold aggregate (integration) |

---

## 12. CI/CD

Pipeline GitHub Actions (`.github/workflows/ci.yml`):

1. **Lint** – `flake8`, `black --check`
2. **Unit Tests** – `pytest tests/ -m "not spark and not integration"`
3. **Build Docker images** – Spark, Alert Engine, Ingestion
4. **Push to Registry** – Tag với Git SHA
5. **Helm Lint** – Validate chart templates
6. **Deploy** – Helm upgrade via ArgoCD (production branch only)
