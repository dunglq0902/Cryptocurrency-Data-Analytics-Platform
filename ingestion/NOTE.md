Chạy Ingestion:
- **Kafka:** nếu cần persist, khởi broker trước: 
`docker-compose up -d zookeeper kafka schema-registry kafka-ui`.  
- **Dependencies:** trong venv chạy `python -m pip install -r requirements.txt`.  
- **Smoke tests:** chạy lại producers nếu cần: 
`python -m ingestion.binance_rest_producer ...` và 
  python -m ingestion.binance_rest_producer --symbols BTCUSDT ETHUSDT --interval 1m --start-date 2026-01-01 --end-date 2026-01-02

`python -m ingestion.binance_ws_producer`.  
  python -m ingestion.binance_ws_producer --symbols BTCUSDT ETHUSDT

- **Verify messages:** lấy vài message mẫu:  
  `docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic raw-ohlcv --from-beginning --max-messages 5 --property print.key=true`  
- **Observability:** theo dõi logs để phát hiện rate-limit / buffer errors / parsing warnings; WS tạo throughput lớn nên theo dõi backpressure.  
- **Tests (tuỳ chọn):** chạy `pytest test_ingestion.py -q` hoặc `pytest -q` để đảm bảo mọi thứ ổn.

=======================================================================

Ingestion layer là nơi:
lấy data từ Binance
đẩy vào Kafka

=======================================================================
✔ Ý nghĩa từng file:
binance_rest_producer.py
→ lấy data từ REST API (pull)

binance_ws_producer.py
→ lấy data real-time từ WebSocket (streaming) 🔥

kafka_config.py
→ config Kafka (bootstrap servers, topic, serializer…)

__init__.py
→ biến folder này thành Python package
=======================================================================

⚠️ 2. _pycache_ là gì?

👉 Đây là folder tự động do Python tạo ra
Khi bạn chạy file .py, Python sẽ:
compile → thành bytecode
lưu vào _pycache_
=======================================================================
1. API REST (Giao thức HTTP)
Dùng REST khi bạn cần lấy một khối lượng lớn dữ liệu "tĩnh" tại một thời điểm nhất định. 
- Batch Processing: Xử lý dữ liệu theo lô, phục vụ phân tích dài hạn.
- Phù hợp tải file JSON/CSV lớn về lịch sử giao dịch.
- Đơn giản, lỗi thì gọi lại (Stateless).
2. WebSocket (WS): Quay video (Streaming) & Thời gian thực
- Stream Processing: Xử lý dữ liệu dòng, phục vụ phân tích thời gian thực.
- Phù hợp nhận hàng nghìn tin nhắn nhỏ mỗi giây về biến động giá.
- Cần code xử lý khi mất kết nối (reconnection logic) và quản lý trạng thái kết nối (Stateful).

=======================================================================
Binance có 2 loại API:
1. Public API — KHÔNG cần API key
Dùng để:

Lấy giá
Lấy lịch sử giao dịch
Lấy order book

👉 Ví dụ:

/api/v3/klines ← bạn đang dùng
/api/v3/ticker/price

➡️ Ai cũng gọi được → không cần API key
2. Private API — CẦN API key

Dùng để:

Đặt lệnh (buy/sell)
Xem tài khoản
Xem số dư

👉 Ví dụ:

/api/v3/account
/api/v3/order

➡️ Phải có API key + secret