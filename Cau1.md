# Câu 1: Tìm hiểu Data Pipeline

## 1. Khái niệm Data Pipeline và các thành phần chính

**Data Pipeline** là chuỗi bước tự động để **thu thập → biến đổi → lưu trữ → phân phối** dữ liệu từ nhiều nguồn đến đích (Data Lake/Data Warehouse/Business Intelligence), đảm bảo tính **đúng – đủ – kịp thời – có thể lặp lại**.

- **Mô hình kiến trúc**:
    - **ETL (Extract → Transform → Load)**
        - Giảm tải cho DWH vì xử lý trước khi lưu.
        - Dữ liệu vào DWH gọn nhẹ, đã sạch.

    - **ELT (Extract → Load → Transform)**
        - Tận dụng sức mạnh tính toán của DWH/Lakehouse (scalable, SQL-based).
        - Linh hoạt: dữ liệu raw luôn có sẵn → có thể transform lại bất kỳ lúc nào.

    - **Bronze / Silver / Gold (Lakehouse)**
        - Lưu dữ liệu gốc từ nguồn (raw).
        - Làm sạch, chuẩn hóa, xử lý dữ liệu thiếu/lỗi (cleaned).
        - Tính toán các KPI, trực quan hoá dữ liệu (BI).


### 1.1 Ingest (Thu thập dữ liệu)
- **Mục tiêu**: Lấy dữ liệu từ nhiều nguồn như cơ sở dữ liệu giao dịch (OLTP), file CSV/JSON, API hoặc hệ thống streaming.
- **Cách tiếp cận**:
  - Batch: Thu thập dữ liệu định kỳ (theo giờ, ngày).
  - Streaming: Dữ liệu được đẩy theo thời gian thực (real-time).
- **Ví dụ**: Trích xuất dữ liệu đơn hàng từ Postgres mỗi 15 phút.

### 1.2 Transform (Xử lý và Biến đổi)
- **Mục tiêu**: Làm sạch dữ liệu, xử lý null, chuẩn hóa kiểu dữ liệu, tính toán các chỉ số (KPIs), chuẩn hóa theo các mô hình dữ liệu chuẩn như Star Schema.
- **Các bước chính**:
  - Làm sạch: Loại bỏ dữ liệu trùng lặp, xử lý dữ liệu thiếu.
  - Chuẩn hóa: Chuyển đổi định dạng ngày giờ, tiền tệ.
  - Tổng hợp: Tính tổng doanh thu, số đơn hàng, số khách hàng.
- **Ví dụ**: Tính cột `net_revenue = total_amount - refund_amount` để phục vụ báo cáo.

### 1.3 Storage (Lưu trữ dữ liệu)
- **Mục tiêu**: Lưu dữ liệu đã xử lý vào hệ thống lưu trữ hiệu quả cho phân tích.
- **Loại lưu trữ**:
  - Data Lake: Lưu dữ liệu thô và bán xử lý (ví dụ: S3, ADLS).
  - Data Warehouse: Lưu dữ liệu đã tối ưu để phân tích nhanh (ví dụ: Snowflake, BigQuery).
- **Ví dụ**: Lưu dữ liệu đã transform vào bảng `fact_sales` trong Data Warehouse.

### 1.4 Orchestration (Điều phối)
- **Mục tiêu**: Quản lý thứ tự chạy, lịch trình, phụ thuộc giữa các bước, xử lý lỗi.
- **Ví dụ công cụ**: Apache Airflow để định nghĩa pipeline bằng DAG, dễ dàng quản lý job, retry khi thất bại.

### 1.5 Monitoring (Giám sát)
- **Mục tiêu**: Theo dõi chất lượng dữ liệu (Data Quality) và tình trạng pipeline (job có chạy thành công không, dữ liệu có bị mất không).
- **Ví dụ**: Cảnh báo qua email hoặc Slack khi pipeline thất bại hoặc dữ liệu thiếu.

---

## 2. So sánh triển khai pipeline bằng Airflow vs n8n

| Tiêu chí | Airflow  | n8n |
|---|---|---|
| Kiểu công cụ | Dựa trên mã Python, hướng Data Engineer | Low-code, kéo-thả cho workflow đơn giản |
| Quy mô | Tốt cho pipeline phức tạp, batch/stream | Phù hợp automation nghiệp vụ nhẹ |
| Lịch trình & Backfill | Rất mạnh, hỗ trợ phụ thuộc phức tạp | Hạn chế hơn, ít hỗ trợ backfill |
| Quản lý & Giám sát | Giao diện chi tiết, log, retry, SLA | Giám sát cơ bản, trực quan |
| Khi nào dùng | Nền tảng dữ liệu doanh nghiệp lớn | Automation đơn giản, MVP nhanh |

**Kết luận**:
- **Airflow**: Phù hợp xây dựng data pipeline quy mô lớn trong công ty, cần lịch trình phức tạp, SLA, giám sát tốt.
- **n8n**: Phù hợp cho tác vụ nhỏ, workflow tự động giữa các dịch vụ SaaS hoặc POC nhanh.

---

## 3. Ví dụ pipeline trong công ty thương mại điện tử: “Đơn hàng → Báo cáo doanh thu”

### 3.1 Yêu cầu
- **Nguồn dữ liệu**: Bảng `orders`, `order_items`, `payments` từ Postgres.
- **Đầu ra**: Báo cáo doanh thu theo ngày/kênh để bộ phận kinh doanh xem trên Power BI.
- **Tần suất**: Cập nhật mỗi 1 giờ.

### 3.2 Kiến trúc tổng quan

```
Postgres (orders/payments)
      │  Ingest (batch/stream)
      ▼
Data Lake (raw zone)
      │  Transform (làm sạch, join, tổng hợp)
      ▼
Data Warehouse (fact_sales, dim_date, dim_product)
      │  Orchestration (Airflow)
      ▼
Báo cáo BI (Power BI, Metabase)
```

### 3.3 Quy trình chi tiết
1. **Ingest**: Airflow task lấy dữ liệu mới từ Postgres → lưu file Parquet vào Data Lake.  
2. **Transform**: Airflow gọi script SQL/Python:  
   - Join bảng `orders` + `payments`  
   - Xử lý hoàn tiền, huỷ đơn  
   - Tính doanh thu thực nhận  
3. **Load vào Data Warehouse**: Lưu kết quả vào bảng `fact_sales` với các cột: `order_id`, `date`, `channel`, `net_revenue`, `refund_amount`, `total_orders`.
4. **Báo cáo**: Power BI truy vấn trực tiếp bảng `fact_sales` để tạo dashboard.

### 3.4 Ví dụ SQL tính doanh thu

```sql
SELECT 
    DATE(order_date) AS order_day,
    SUM(total_amount - refund_amount) AS net_revenue,
    COUNT(DISTINCT order_id) AS total_orders
FROM fact_sales
WHERE order_date >= CURRENT_DATE - INTERVAL '7 day'
GROUP BY DATE(order_date)
ORDER BY order_day;
```

### 3.5 Orchestration với Airflow (minh hoạ DAG)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Extract
def ingest_data():
    print("Lấy dữ liệu từ Postgres và lưu vào Data Lake")

# Transform
def transform_data():
    print("Xử lý dữ liệu, tính doanh thu")

# Load
def load_to_dwh():
    print("Lưu dữ liệu vào Data Warehouse")

with DAG(
    'ecommerce_revenue_pipeline',
    start_date=datetime(2025, 9, 26),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    ingest = PythonOperator(task_id='ingest', python_callable=ingest_data)
    transform = PythonOperator(task_id='transform', python_callable=transform_data)
    load = PythonOperator(task_id='load', python_callable=load_to_dwh)

    ingest >> transform >> load
```

### 3.6 Monitoring
- Airflow UI hiển thị trạng thái task thành công/thất bại.
- Cảnh báo qua ứng dụng (Email, MS Teams, Slack) khi pipeline lỗi hoặc dữ liệu bị thiếu.

---
