# Hướng dẫn Chạy Dự án trên Kubernetes (RUN.md)
---

## 1. Triển khai Hệ thống (Deployment)

Mở PowerShell tại thư mục gốc của dự án và chạy:
```powershell

# Chạy deployment script
powershell -ExecutionPolicy Bypass -File k8s/deploy.ps1

```

## 2. Câu lệnh Port-Forward truy cập Techstack
Lưu ý: Mở nhiều terminal để chạy Lệnh Port-Forward

| Dịch vụ | Lệnh Port-Forward | Địa chỉ truy cập trên Windows |
| **Streamlit Dashboard** | 
kubectl port-forward service/dashboard -n crypto-analytics 30501:8501
http://localhost:30501

| **Spark Master UI** | 
kubectl port-forward service/spark-master -n crypto-analytics 8080:8080
http://localhost:8080

| **MinIO Console** | 
kubectl port-forward pod/minio-0 -n crypto-analytics 9001:9001
http://localhost:9001

| **MongoDB Connection** | 
kubectl port-forward pod/mongodb-0 -n crypto-analytics 27017:27017
mongodb://root:changeme@localhost:27017 (Dùng Compass kết nối) |


## 3. Kiểm tra Trạng thái và Xem Logs (Troubleshooting)

### 3.1. Kiểm tra trạng thái các Pods và Services
```bash
# Xem tất cả tài nguyên đang chạy trong namespace
kubectl get all -n crypto-analytics

# Xem chi tiết trạng thái các Pods
kubectl get pods -n crypto-analytics -o wide
```


### 3.2. Xem Logs thời gian thực của các dịch vụ quan trọng

* **Logs dữ liệu Ingestion (Binance WS -> Kafka):**
kubectl logs deployment/ingestion-service -n crypto-analytics -f --tail=100

* **Logs xử lý dữ liệu của Spark (Bronze -> Silver -> Gold):**
kubectl logs deployment/spark-unified-streaming -n crypto-analytics -f --tail=100

* **Logs của Alert Engine (Bắt cảnh báo và chuyển đi):**
kubectl logs deployment/alert-consumer -n crypto-analytics -f --tail=50

* **Logs của Notification Service (Gửi Email/Webhook):**
kubectl logs deployment/notification-service -n crypto-analytics -f --tail=50

---



## 4. Gỡ bỏ Hệ thống (Teardown)

Khi muốn tắt hoàn toàn dự án và giải phóng RAM/CPU trên máy tính:

### Cách 1: Sử dụng Script tự động
```powershell
powershell -ExecutionPolicy Bypass -File k8s/teardown.ps1
```

### Cách 2: Xóa thủ công bằng lệnh
```bash
kubectl delete -f k8s/05-apps.yaml --ignore-not-found
kubectl delete -f k8s/04-spark.yaml --ignore-not-found
kubectl delete -f k8s/03-init-jobs.yaml --ignore-not-found
kubectl delete -f k8s/02-infra.yaml --ignore-not-found
kubectl delete -f k8s/01-secrets.yaml --ignore-not-found
kubectl delete pvc --all -n crypto-analytics --ignore-not-found
kubectl delete -f k8s/00-namespace.yaml --ignore-not-found
```
