# Cryptocurrency Data Analytics Platform
# 🧱 1. Tổng thể cấu trúc

```bash
crypto-streaming-lakehouse/
│
├── README.md
├── docker-compose.yml
├── .env
├── requirements.txt
│
├── infrastructure/          # Hạ tầng (Kafka, Spark, DB, K8s)
├── ingestion/               # Lấy data từ Binance → Kafka
├── streaming/               # Spark Structured Streaming
├── batch/                   # Batch processing (reprocess / ML training)
├── lakehouse/               # Data Lake (Iceberg/Delta + Medallion)
├── warehouse/               # (Optional) PostgreSQL / Analytics
├── nosql/                   # MongoDB / Cassandra
├── ml/                      # MLlib pipeline
├── orchestration/           # Airflow DAGs
├── api/                     # Serve dữ liệu / model
├── dashboard/               # Visualization
├── tests/                   # Testing
└── docs/                    # Architecture + report
```

---

# 🚀 2. Chi tiết từng phần

---

# 🏗️ infrastructure/

```bash
infrastructure/
├── docker/
│   ├── kafka/
│   ├── spark/
│   ├── airflow/
│   ├── mongodb/
│   └── postgres/
│
├── kubernetes/
│   ├── kafka/
│   ├── spark/
│   ├── airflow/
│   ├── mongodb/
│   └── ingress/
│
└── terraform/ (optional - nếu dùng cloud)
```

👉 Vai trò:

* Setup toàn bộ hệ thống
* Docker → local
* Kubernetes → production

---

# 📡 ingestion/ (Binance → Kafka)

```bash
ingestion/
├── producers/
│   ├── binance_ws_producer.py
│   ├── binance_rest_producer.py
│
├── schemas/
│   └── trade_schema.json
│
├── utils/
│   ├── config.py
│   └── logger.py
│
└── run_producer.py
```

👉 Bạn sẽ:

* Stream giá BTC/ETH real-time vào Kafka

---

# 🔥 streaming/ (project core)

```bash
streaming/
├── jobs/
│   ├── stream_to_bronze.py
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│
├── transformations/
│   ├── cleaning.py
│   ├── aggregation.py
│   └── feature_engineering.py
│
├── schemas/
│   └── spark_schema.py
│
├── sinks/
│   ├── iceberg_sink.py
│   ├── mongodb_sink.py
│
└── utils/
    ├── spark_session.py
    └── config.py
```

👉 Đây là:

* **Trái tim của Streaming Lakehouse**
* Spark đọc Kafka → ghi vào Lakehouse

---

# 🧊 lakehouse/ (Medallion Architecture)

```bash
lakehouse/
├── bronze/     # Raw data
│   └── kafka_raw/
│
├── silver/     # Cleaned data
│   └── trades_cleaned/
│
├── gold/       # Aggregated / business-ready
│   ├── price_agg/
│   └── indicators/
│
├── schemas/
│   └── table_definitions.sql
│
└── catalog/
    └── iceberg_catalog_config.yaml
```

👉 Mapping:

* Bronze → raw từ Kafka
* Silver → clean
* Gold → dùng cho ML / dashboard

---

# 🗄️ warehouse/ (optional)

```bash
warehouse/
├── models/
│   ├── fact_trades.sql
│   ├── dim_time.sql
│
├── etl/
│   └── load_to_postgres.py
│
└── queries/
    └── analytics.sql
```

👉 Dùng để:

* Query nhanh
* Demo SQL

---

# 🍃 nosql/

```bash
nosql/
├── mongodb/
│   ├── models/
│   │   └── trade_model.py
│   ├── insert_stream.py
│   └── queries.py
```

👉 Use case:

* Lưu real-time data
* API đọc nhanh

---

# 🤖 ml/

```bash
ml/
├── training/
│   ├── train_model.py
│   ├── feature_pipeline.py
│
├── inference/
│   └── predict.py
│
├── models/
│   └── saved_model/
│
└── utils/
    └── metrics.py
```

👉 MLlib:

* Predict giá crypto
* Feature từ Gold layer

---

# ⏰ orchestration/ (Airflow)

```bash
orchestration/
├── dags/
│   ├── streaming_pipeline_dag.py
│   ├── batch_training_dag.py
│
├── plugins/
└── configs/
```

👉 Dùng để:

* Schedule ML
* Reprocess data

---

# 🌐 api/

```bash
api/
├── app.py
├── routes/
│   ├── price.py
│   ├── prediction.py
│
├── services/
│   ├── mongo_service.py
│   └── model_service.py
```

👉 Serve:

* Giá real-time
* Prediction

---

# 📊 dashboard/

```bash
dashboard/
├── superset/
│   └── config.py
│
└── grafana/
    └── dashboards/
```

---

# 🧪 tests/

```bash
tests/
├── test_ingestion.py
├── test_streaming.py
├── test_ml.py
```

---

# 📚 docs/

```bash
docs/
├── architecture.png
├── data_flow.md
├── setup_guide.md
└── report.pdf
```

---

==============================================================================================
# 🧠 1. CORE TECH STACK (BẮT BUỘC – để đúng kiến trúc)

## 🔴 Streaming & Event Backbone
* Apache Kafka
  👉 Nhận data từ Binance → backbone của toàn hệ thống

---
## 🔵 Processing Engine

* Apache Spark (Structured Streaming)

👉 Làm:

* Streaming (real-time)
* Batch (reprocess)
* ML (MLlib)

---

## 🧊 Lakehouse Storage

Chọn **1 trong 2** (không cần cả 2):

* Apache Iceberg ⭐ (khuyên dùng)
  hoặc
* Delta Lake

👉 Đây là thứ biến:

* S3/HDFS → **Lakehouse (có ACID, query được)**

---

## 🗂️ Storage Layer

* Hadoop Distributed File System
  hoặc
* Amazon S3 (hoặc MinIO local)

---

## 🍃 NoSQL (theo yêu cầu môn)

* MongoDB

👉 Dùng:

* Lưu dữ liệu real-time
* API query nhanh

---

## 🤖 Machine Learning

* Spark MLlib

👉 Train model trực tiếp từ Gold layer

---

# 🚀 2. SUPPORTING TECH (NÊN CÓ – để “ăn điểm”)

## 🐳 Container

* Docker

👉 Chạy toàn bộ hệ thống local

---

## ☸️ Orchestration (bắt buộc đề bài)

* Kubernetes

👉 Deploy:

* Kafka
* Spark
* MongoDB

---

## ⏰ Workflow

* Apache Airflow

👉 Schedule:

* Batch job
* ML training

---

# 📊 3. VISUALIZATION & SERVING (optional)

## Dashboard

* Apache Superset
  hoặc
* Grafana

---

## API

* Python:

  * FastAPI / Flask

👉 Serve:

* Giá crypto
* Prediction

---

# 🧱 4. OPTIONAL (advanced)

## SQL Query Engine

* Trino

👉 Query trực tiếp trên Iceberg

---

## Data Warehouse (nếu muốn hybrid)

* PostgreSQL

---

## Message Schema

* Apache Avro

---

# 🧩 5. Mapping với thư mục bạn đã có

| Folder          | Công nghệ               |
| --------------- | ----------------------- |
| ingestion/      | Python + Kafka          |
| streaming/      | Spark                   |
| lakehouse/      | Iceberg + HDFS          |
| nosql/          | MongoDB                 |
| ml/             | Spark MLlib             |
| orchestration/  | Airflow                 |
| infrastructure/ | Docker + Kubernetes     |
| api/            | FastAPI                 |
| dashboard/      | Superset/Grafana        |

---
