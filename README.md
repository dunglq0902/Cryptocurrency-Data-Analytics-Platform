# Cryptocurrency Data Analytics Platform

> **Streaming Lakehouse Architecture | Real-time Trading Signals**  
> Big Data Systems
---

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Kiến trúc hệ thống](#2-kiến-trúc-hệ-thống)
3. [Cấu trúc thư mục](#3-cấu-trúc-thư-mục)
4. [Stack công nghệ](#4-stack-công-nghệ)
5. [Yêu cầu môi trường](#5-yêu-cầu-môi-trường)
6. [Quickstart – Local Development](#6-quickstart--local-development)
7. [Kubernetes Deployment](#7-kubernetes-deployment)
8. [Cấu hình chi tiết](#8-cấu-hình-chi-tiết)
9. [Apache Spark Jobs](#9-apache-spark-jobs)
10. [Alert Engine](#10-alert-engine)
11. [Airflow DAGs](#11-airflow-dags)
12. [Monitoring](#12-monitoring)
13. [Tests](#13-tests)
14. [CI/CD](#14-cicd)

---

## 1. Tổng quan

**Cryptocurrency Data Analytics Platform** là hệ thống xử lý dữ liệu lớn toàn diện, thu thập và phân tích dữ liệu thị trường tiền mã hóa theo thời gian thực từ sàn giao dịch Binance. Hệ thống áp dụng kiến trúc **Streaming Lakehouse** kết hợp với **Medallion Architecture** (Bronze → Silver → Gold) và cung cấp hệ thống cảnh báo tín hiệu giao dịch (mua/bán) có khả năng cá nhân hóa cao.

### Tính năng chính

| Tính năng | Mô tả |
|---|---|
| **Real-time Ingestion** | WebSocket streaming từ Binance (tick data, OHLCV, order book) |
| **Batch Backfill** | REST API producer để nạp dữ liệu lịch sử |
| **Streaming ETL** | Spark Structured Streaming với watermark, exactly-once |
| **Technical Indicators** | RSI, MACD, Bollinger Bands, MA, ATR, Volume Profile |
| **Alert Engine** | Hệ thống cảnh báo mua/bán với điều kiện lọc tùy chỉnh |
| **Notifications** | Telegram Bot, Email (SMTP), Webhook |
| **Orchestration** | Apache Airflow với 8 DAGs tự động hóa toàn bộ pipeline |
| **Monitoring** | Grafana dashboards + Prometheus metrics + alerting rules |

---

## 2. Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                              │
│   Binance WebSocket ──► Kafka (raw-ohlcv, raw-ticks, raw-orderbook) │
│   Binance REST API  ──► Kafka (historical backfill)                 │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ Spark Structured Streaming
┌─────────────────────────────▼───────────────────────────────────────┐
│                      PROCESSING LAYER                               │
│  Bronze (raw)  ──►  Silver (cleaned + indicators)  ──►  Gold (agg) │
│  HDFS/Delta         HDFS/Delta Lake                  HDFS/Delta     │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
       ┌──────────────────────┼──────────────────────┐
       ▼                      ▼                      ▼
┌─────────────┐    ┌──────────────────┐    ┌────────────────────┐
│   Grafana   │    │  Alert Engine    │    │   Superset / BI    │
│  Dashboards │    │  FastAPI + Rules │    │    SQL Analytics   │
└─────────────┘    └────────┬─────────┘    └────────────────────┘
                            │
               ┌────────────┴────────────┐
               ▼                         ▼
        Telegram Bot               Email / Webhook
```

### Medallion Architecture

| Zone | Description | Format |
|---|---|---|
| **Bronze** | Raw data từ Kafka, không biến đổi, có metadata | Parquet / Delta |
| **Silver** | Đã làm sạch, normalize, technical indicators | Delta Lake |
| **Gold** | Multi-timeframe OHLCV, VWAP, window functions | Delta Lake (Z-ordered) |

---

## 3. Cấu trúc thư mục

```
crypto-analytics-platform/
│
├── ingestion/                      # Binance API producers
│   ├── binance_ws_producer.py      # WebSocket streaming (tick data)
│   ├── binance_rest_producer.py    # REST API (historical OHLCV)
│   └── kafka_config.py             # Kafka settings, topic definitions
│
├── spark/                          # PySpark jobs & utilities
│   ├── jobs/
│   │   ├── bronze_ingest.py        # Kafka → Bronze Delta (streaming)
│   │   ├── silver_clean.py         # Bronze → Silver (6-stage pipeline)
│   │   ├── gold_aggregate.py       # Silver → Gold (multi-timeframe)
│   │   └── alert_evaluator.py      # Gold → Alert signals
│   ├── udfs/
│   │   └── indicator_udfs.py       # RSI, MACD, candle classifier UDFs
│   ├── schemas/
│   │   └── bronze_schema.py        # Spark DataFrame schemas
│   └── utils/
│       └── spark_session.py        # SparkSession factory + helpers
│
├── airflow/
│   ├── dags/                       # 8 DAGs covering full pipeline lifecycle
│   │   ├── crypto_bronze_ingestion.py
│   │   ├── crypto_silver_processing.py
│   │   ├── crypto_gold_aggregation.py
│   │   ├── crypto_alert_evaluation.py
│   │   ├── crypto_historical_backfill.py
│   │   ├── crypto_data_quality.py
│   │   ├── crypto_model_training.py   # includes crypto_cleanup DAG
│   │   └── crypto_silver_processing.py
│   └── plugins/
│       └── kafka_sensor.py         # Custom KafkaTopicSensor
│
├── alert-engine/
│   ├── api/
│   │   ├── main.py                 # FastAPI app (CRUD + dispatch endpoint)
│   │   ├── models.py               # Pydantic models for rules & events
│   │   └── routes/
│   │       └── rules.py            # /api/v1/rules CRUD router
│   ├── evaluator/
│   │   └── rule_engine.py          # Pure-Python rule condition evaluator
│   └── notifier/
│       ├── notifiers.py            # Telegram, Email, Webhook dispatchers
│       └── notification_service.py # FastAPI notification microservice
│
├── k8s/                            # Kubernetes manifests
│   ├── namespaces/namespaces.yaml
│   ├── kafka/kafka-statefulset.yaml
│   ├── spark/spark-applications.yaml
│   ├── spark/alert-engine.yaml
│   ├── airflow/airflow-values.yaml
│   └── monitoring/monitoring.yaml
│
├── docker/                         # Dockerfiles & requirements
│   ├── Dockerfile.spark
│   ├── Dockerfile.alert-engine
│   ├── Dockerfile.ingestion
│   ├── spark-defaults.conf
│   ├── requirements.spark.txt
│   ├── requirements.alert-engine.txt
│   └── requirements.ingestion.txt
│
├── helm/
│   ├── infra/                      # Chart for Kafka, HDFS, MongoDB
│   │   ├── Chart.yaml
│   │   └── values.yaml
│   └── apps/                       # Chart for Alert Engine, Airflow, Ingestion
│       ├── Chart.yaml
│       └── values.yaml
│
├── monitoring/
│   ├── prometheus/
│   │   ├── prometheus.yml          # Prometheus scrape configuration
│   │   └── alert_rules.yml         # Alerting rules (Kafka, Spark, DQ, system)
│   └── grafana/
│       ├── dashboard_crypto_prices.json   # Real-time price + RSI + alerts
│       └── dashboard_kafka_spark.json     # Kafka lag + Spark performance
│
├── tests/
│   ├── conftest.py                 # Shared pytest fixtures
│   ├── test_ingestion.py           # Unit tests for producers & parsers
│   ├── test_alert_engine.py        # Unit tests for rule evaluator & models
│   ├── test_spark_udfs.py          # Unit tests for UDF logic
│   ├── test_spark_jobs.py          # Integration tests (requires PySpark)
│   └── test_alert_api.py           # API endpoint tests (TestClient)
│
├── scripts/
│   ├── setup.sh                    # One-shot local dev setup
│   ├── teardown.sh                 # Stop and optionally remove volumes
│   ├── seed_data.py                # Seed MongoDB with sample alert rules
│   └── submit_spark_job.sh         # spark-submit helper (local + K8s)
│
├── docker-compose.yml              # Local development stack
├── .env.example                    # Environment variable template
└── README.md                       # This file
```

---

## 4. Stack công nghệ

| Thành phần | Công nghệ | Phiên bản |
|---|---|---|
| Data Source | Binance WebSocket/REST API | v3 |
| Message Queue | Apache Kafka + Schema Registry | 3.6+ / 7.5 |
| Stream Processing | Apache Spark (PySpark) | 3.5+ |
| Distributed Storage | HDFS + Delta Lake | 3.x |
| NoSQL Database | MongoDB | 7.x |
| Orchestration | Apache Airflow | 2.8+ |
| Alert Engine | FastAPI + Motor | 0.109 / 3.3 |
| Containerization | Docker | 24+ |
| Orchestration Platform | Kubernetes (Minikube/EKS) | 1.28+ |
| Monitoring | Prometheus + Grafana | 2.49 / 10.3 |
| Language | Python | 3.11+ |

---

## 5. Yêu cầu môi trường

### Local Development

| Công cụ | Phiên bản tối thiểu |
|---|---|
| Docker Desktop | 24.0+ |
| Python | 3.11+ |
| Java (OpenJDK) | 11+ |
| Git | 2.40+ |

### Production (Kubernetes)

| Công cụ | Phiên bản tối thiểu |
|---|---|
| kubectl | 1.28+ |
| Minikube (dev) hoặc EKS/GKE | 1.32+ |
| Helm | 3.13+ |

---

## 6. Quickstart – Local Development

### Bước 1: Clone và cấu hình

```bash
git clone https://github.com/your-org/crypto-analytics-platform.git
cd crypto-analytics-platform

# Tạo file .env từ template
cp .env.example .env
# Điền Binance API keys và Telegram bot token vào .env
```

### Bước 2: Setup tự động (khuyến nghị)

```bash
bash scripts/setup.sh
```

Script này sẽ tự động:
- Kiểm tra prerequisites
- Khởi động toàn bộ stack với docker-compose
- Tạo Kafka topics
- Khởi tạo MongoDB indexes
- Chạy unit tests

### Bước 3: Chạy thủ công từng bước

```bash
# 1. Khởi động infrastructure
docker-compose up -d

# 2. Khởi động Binance WebSocket producer
python ingestion/binance_ws_producer.py --symbols BTCUSDT ETHUSDT --interval 1m

# 3. Submit Bronze Spark job (terminal mới)
bash scripts/submit_spark_job.sh bronze

# 4. Submit Silver Spark job (sau khi Bronze chạy)
bash scripts/submit_spark_job.sh silver --date $(date +%Y-%m-%d)

# 5. Submit Gold Spark job
bash scripts/submit_spark_job.sh gold --date $(date +%Y-%m-%d)

# 6. Khởi động Alert Engine API
uvicorn alert-engine.api.main:app --host 0.0.0.0 --port 8000 --reload

# 7. Seed sample alert rules
python scripts/seed_data.py
```

### Bước 4: Truy cập các giao diện

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Spark Master UI | http://localhost:8082 | — |
| Airflow Webserver | http://localhost:8081 | admin / admin |
| Alert Engine API | http://localhost:8000/docs | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| HDFS NameNode | http://localhost:9870 | — |

---

## 7. Kubernetes Deployment

### Setup Minikube

```bash
# Khởi động Minikube với đủ resources
minikube start --cpus=6 --memory=12288 --disk-size=50g

# Bật addons cần thiết
minikube addons enable metrics-server
minikube addons enable ingress
```

### Deploy Infrastructure

```bash
# Thêm Helm repos
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Tạo namespaces
kubectl apply -f k8s/namespaces/namespaces.yaml

# Deploy infrastructure (Kafka, MongoDB, HDFS)
helm upgrade --install crypto-infra ./helm/infra \
  --namespace kafka-system \
  --values helm/infra/values.yaml

# Deploy Kafka cluster
kubectl apply -f k8s/kafka/kafka-statefulset.yaml -n kafka-system

# Deploy Monitoring
kubectl apply -f k8s/monitoring/monitoring.yaml
```

### Deploy Applications

```bash
# Deploy Spark Operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-system \
  --set sparkJobNamespace=spark-system

# Deploy Spark jobs
kubectl apply -f k8s/spark/spark-applications.yaml -n spark-system

# Deploy Alert Engine
kubectl apply -f k8s/spark/alert-engine.yaml -n app-system

# Deploy Airflow
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow-system \
  --values k8s/airflow/airflow-values.yaml

# Deploy full app stack
helm upgrade --install crypto-apps ./helm/apps \
  --namespace app-system \
  --values helm/apps/values.yaml
```

### Kiểm tra trạng thái

```bash
# Kiểm tra tất cả pods
kubectl get pods --all-namespaces

# Port-forward Grafana để xem dashboard
kubectl port-forward svc/grafana 3000:3000 -n monitoring-system

# Port-forward Airflow UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow-system

# Xem logs Spark job
kubectl logs -n spark-system -l spark-role=driver
```

---

## 8. Cấu hình chi tiết

### Environment Variables quan trọng

| Biến | Mô tả | Giá trị mặc định |
|---|---|---|
| `BINANCE_API_KEY` | Binance API key | — |
| `BINANCE_API_SECRET` | Binance API secret | — |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:9092` |
| `HDFS_NAMENODE` | HDFS NameNode URI | `hdfs://namenode:8020` |
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017` |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API token | — |
| `SPARK_MASTER` | Spark master URL | `local[*]` |

### Kafka Topics

| Topic | Partitions | Retention | Mô tả |
|---|---|---|---|
| `raw-crypto-ticks` | 5 | 7 ngày | Individual trade events |
| `raw-ohlcv` | 5 | 7 ngày | 1-min OHLCV candles |
| `raw-orderbook` | 5 | 1 ngày | Best bid/ask |
| `processed-signals` | 5 | 30 ngày | Processed trading signals |
| `alert-events` | 5 | 30 ngày | Alert trigger events |

---

## 9. Apache Spark Jobs

### Tổng quan 4 jobs

| Job | File | Input | Output | Mode |
|---|---|---|---|---|
| **Bronze Ingest** | `bronze_ingest.py` | Kafka | Bronze Delta | Streaming |
| **Silver Clean** | `silver_clean.py` | Bronze Delta | Silver Delta | Batch + Streaming |
| **Gold Aggregate** | `gold_aggregate.py` | Silver Delta | Gold Delta | Batch + Streaming |
| **Alert Evaluator** | `alert_evaluator.py` | Gold Delta | Kafka `alert-events` | Streaming |

### Chạy jobs thủ công

```bash
# Bronze – streaming (chạy liên tục)
bash scripts/submit_spark_job.sh bronze

# Silver – batch cho ngày cụ thể
bash scripts/submit_spark_job.sh silver --date 2024-01-15 --mode batch

# Gold – batch
bash scripts/submit_spark_job.sh gold --date 2024-01-15 --mode batch

# Alert Evaluator – streaming
bash scripts/submit_spark_job.sh alert
```

### Technical Indicators được tính

| Indicator | Mô tả | Window |
|---|---|---|
| MA7, MA25, MA99 | Moving Averages | 7, 25, 99 nến |
| Bollinger Bands | Upper/Middle/Lower band | 20 nến, 2σ |
| RSI(14) | Relative Strength Index | 14 nến |
| MACD | 12-26-9 | EMA-based |
| ATR(14) | Average True Range | 14 nến |
| Volume Ratio | Volume / MA(20) volume | 20 nến |

---

## 10. Alert Engine

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
GET    /metrics                        # Prometheus metrics
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
    "notification_channels": ["telegram"],
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

---

## 11. Airflow DAGs

| DAG | Schedule | Mô tả |
|---|---|---|
| `crypto_bronze_ingestion` | Mỗi 1 phút | Monitor Kafka lag → trigger Bronze streaming job |
| `crypto_silver_processing` | Mỗi 5 phút | Bronze → Silver cleaning + indicators |
| `crypto_gold_aggregation` | Mỗi 15 phút | Silver → Gold multi-timeframe OHLCV |
| `crypto_alert_evaluation` | Mỗi 1 phút | Evaluate rules → dispatch notifications |
| `crypto_historical_backfill` | Hàng ngày | Backfill lịch sử từ Binance REST API |
| `crypto_data_quality` | Hàng giờ | Kiểm tra null rate, freshness, row count |
| `crypto_model_training` | Hàng tuần | Spark MLlib RandomForest + K-Means |
| `crypto_cleanup` | Hàng ngày 00:00 | Delta VACUUM + OPTIMIZE |

---

## 12. Monitoring

### Grafana Dashboards

| Dashboard | Mô tả |
|---|---|
| **Real-time Prices** | Candlestick prices, RSI, Volume Ratio, Alert count |
| **Kafka & Spark** | Consumer lag, throughput, batch duration, memory |

Truy cập Grafana tại `http://localhost:3000` (admin/admin).

### Prometheus Metrics chính

| Metric | Mô tả |
|---|---|
| `kafka_consumer_group_lag` | Consumer lag per topic partition |
| `spark_streaming_last_completed_batch_processing_time_ms` | Batch latency |
| `alert_engine_dispatched_total` | Total alerts dispatched |
| `crypto_dq_freshness_seconds` | Data freshness per layer |
| `crypto_dq_row_count` | Row count per Delta table |

---

## 13. Tests

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
| `alert-engine/evaluator/` | `test_alert_engine.py` – conditions, AND/OR logic, batch eval |
| `alert-engine/api/` | `test_alert_api.py` – CRUD endpoints, validation |
| `spark/udfs/` | `test_spark_udfs.py` – candle classifier, RSI, formatters |
| `spark/jobs/` | `test_spark_jobs.py` – Silver clean, Gold aggregate (integration) |

---

## 14. CI/CD

Pipeline GitHub Actions (`.github/workflows/ci.yml`):

1. **Lint** – `flake8`, `black --check`
2. **Unit Tests** – `pytest tests/ -m "not spark and not integration"`
3. **Build Docker images** – Spark, Alert Engine, Ingestion
4. **Push to Registry** – Tag với Git SHA
5. **Helm Lint** – Validate chart templates
6. **Deploy** – Helm upgrade via ArgoCD (production branch only)

---

## Lưu ý

- File `.env` chứa API keys nhạy cảm — **không commit** vào Git.
- Cấu hình Kubernetes secrets trong production bằng **Sealed Secrets** hoặc **Vault**.
- Delta Lake VACUUM giữ 7 ngày lịch sử (Bronze), 14 ngày (Silver), 30 ngày (Gold).
- Spark streaming jobs sử dụng **RocksDB State Store** và checkpoint trên HDFS — đảm bảo HDFS luôn khả dụng trước khi submit jobs.

---
