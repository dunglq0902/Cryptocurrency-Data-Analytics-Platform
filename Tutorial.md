1. Kích hoạt môi trường ảo
Khi bạn chạy lệnh 
```
.venv\Scripts\activate
```
 trong terminal
, bạn đang ra lệnh cho Terminal: "Hãy tạm thời quên đi Python cài chung của cả máy tính, và chỉ sử dụng Python cùng các thư viện nằm trong thư mục .venv này thôi."

2. Cài đặt thư viện: Lúc này bạn chỉ cần gõ lệnh ngắn gọn:
Ví dụ: pip install kafka-python

3. Xem danh sách các thư viện trong môi trường ảo đã cài
pip list

4. Xuất danh sách thư viện ra file (Freeze)
```
pip freeze > requirements.txt
```
Lệnh này có ý nghĩa gì?
pip freeze: Liệt kê tất cả các thư viện và phiên bản chính xác đang có trong .venv của bạn.
>: Ghi đè toàn bộ danh sách đó vào file requirements.txt.
Bây giờ bạn mở file requirements.txt lên, bạn sẽ thấy nó không còn trống nữa mà đầy ắp các thư viện kèm phiên bản (ví dụ: kafka-python==2.0.2).

5. Cài tất cả các thư viện trong dự án
pip install -r requirements.txt
