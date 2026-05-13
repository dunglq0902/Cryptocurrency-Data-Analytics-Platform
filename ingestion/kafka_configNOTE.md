Hiện tại project của bạn đang cấu hình Kafka theo kiểu **1 cấu hình chung + nhiều runtime context** (local và K8s). Mình tóm tắt theo đúng các ý bạn hỏi:

## 1) Broker đang dùng

- **Mặc định local**: `localhost:9092` (trong `ingestion` và `spark` nếu không set env)
- **Khi chạy Airflow/K8s**: `kafka-service.kafka-system.svc.cluster.local:9092`
- Hạ tầng Kafka trên K8s là **3 broker + 3 zookeeper**, service chính là `kafka-service` (NodePort `30092`)

```15:17:ingestion/kafka_config.py
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
```

```193:203:k8s/kafka/kafka-statefulset.yaml
  name: kafka-service
  namespace: kafka-system
spec:
  type: NodePort
  selector:
    app: kafka
  ports:
    - port: 9092
      targetPort: 9092
      nodePort: 30092
```

---

## 2) Topic đang định nghĩa

Trong cấu hình trung tâm có 5 topic:

- `raw-crypto-ticks`
- `raw-ohlcv`
- `raw-orderbook`
- `processed-signals`
- `alert-events`

```24:30:ingestion/kafka_config.py
TOPICS = {
    "raw_ticks":        "raw-crypto-ticks",
    "raw_ohlcv":        "raw-ohlcv",
    "raw_orderbook":    "raw-orderbook",
    "processed_signals":"processed-signals",
    "alert_events":     "alert-events",
}
```

---

## 3) Producer hiện có

### Producer 1: WebSocket producer (`ingestion/binance_ws_producer.py`)
Luồng mapping event -> topic:

- `kline` -> `raw-ohlcv`
- `trade` -> `raw-crypto-ticks`
- `bookTicker` -> `raw-orderbook`

```177:188:ingestion/binance_ws_producer.py
if event_type == "kline":
    parsed = parse_kline_event(raw)
    topic  = TOPICS["raw_ohlcv"]
elif event_type == "trade":
    parsed = parse_trade_event(raw)
    topic  = TOPICS["raw_ticks"]
elif event_type == "bookTicker" or "b" in raw and "a" in raw and "s" in raw:
    parsed = parse_book_ticker_event(raw)
    topic  = TOPICS["raw_orderbook"]
```

### Producer 2: REST producer (`ingestion/binance_rest_producer.py`)
- Chỉ produce OHLCV lịch sử vào `raw-ohlcv`

```227:234:ingestion/binance_rest_producer.py
self.producer.produce(
    topic=TOPICS["raw_ohlcv"],
    value=value_bytes,
    key=key_bytes,
    # partition=partition,
    callback=_delivery_report,
)
```

### Producer 3: Alert evaluator Spark job (`spark/jobs/alert_evaluator.py`)
- Sau khi evaluate rule từ Gold layer, ghi alert vào `alert-events`

```418:422:spark/jobs/alert_evaluator.py
.write
.format("kafka")
.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
.option("topic", ALERT_TOPIC)
.save()
```

---

## 4) Consumer hiện có

### Consumer 1: Spark Bronze ingest (`spark/jobs/bronze_ingest.py`)
- Consume:
  - `raw-ohlcv`
  - `raw-crypto-ticks`
- Sau đó ghi vào Bronze Delta (`bronze/ohlcv`, `bronze/ticks`)

```48:53:spark/jobs/bronze_ingest.py
spark.readStream
.format("kafka")
.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
.option("subscribe", KAFKA_TOPICS_OHLCV)
.option("startingOffsets", "latest")
```

### Consumer 2: Airflow dispatch notifications (`airflow/dags/crypto_alert_evaluation.py`)
- Consume `alert-events` bằng consumer group `airflow-alert-dispatcher`
- Đẩy tiếp sang Notification API

```45:52:airflow/dags/crypto_alert_evaluation.py
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id":          "airflow-alert-dispatcher",
    "auto.offset.reset": "latest",
    "enable.auto.commit":False,
})
consumer.subscribe([ALERT_TOPIC])
```

### Consumer 3: Airflow Kafka sensor (`airflow/plugins/kafka_sensor.py`)
- Không consume business data để xử lý, mà dùng để **đo lag/offset** cho nhóm consumer.

---

## 5) Luồng dữ liệu vào/ra topic (end-to-end)

- **Binance WebSocket**
  - `trade` -> `raw-crypto-ticks`
  - `kline` -> `raw-ohlcv`
  - `bookTicker` -> `raw-orderbook`
- **Binance REST backfill**
  - OHLCV lịch sử -> `raw-ohlcv`
- **Spark Bronze ingest**
  - đọc `raw-ohlcv`, `raw-crypto-ticks` -> Bronze Delta
- **Silver/Gold jobs**
  - xử lý từ Delta (không đọc Kafka trực tiếp trong code hiện tại)
- **Spark Alert Evaluator**
  - đọc Gold Delta + rules MongoDB -> ghi `alert-events`
- **Airflow dispatch**
  - đọc `alert-events` -> gọi Notification service

---

## 6) Điểm quan trọng bạn nên lưu ý

- `CONSUMER_CONFIG` trong `kafka_config.py` hiện **chưa thấy nơi nào dùng trực tiếp**.
- `processed-signals` được khai báo nhưng **chưa thấy producer/consumer thực tế** trong code hiện tại.
- `raw-orderbook` có producer (WS), nhưng **chưa thấy consumer pipeline downstream** tương ứng.
- Có bật `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` ở K8s, nên topic có thể được tạo tự động khi produce.

Nếu bạn muốn, mình có thể vẽ tiếp cho bạn một bảng “Topic -> Producer -> Consumer -> Storage/API” để bạn dùng như checklist vận hành.