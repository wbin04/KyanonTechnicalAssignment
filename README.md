# Kyanon Technical Assignment

## Cấu trúc thư mục

```
KyanonTechnicalAssignment
├── [Revised] Kyanon Digital - Data Engineer Intern - Entrance Assessment.pdf   # Đề bài
├── Cau1.md                                                                     # Câu 1: Tìm hiểu Data Pipeline
├── Cau2.py                                                                     # Câu 2: Bài tập kỹ thuật (Data Aggregation)
├── Cau3.md                                                                     # Câu 3: Nội dung email
├── requirements.txt                                                            # Danh sách thư viện cần thiết
├── data_input/                                                                 # Thư mục chứa file CSV đầu vào
│   └── orders.csv                                                              # Dữ liệu đơn hàng mẫu
└── data_output/                                                                # Thư mục chứa file CSV kết quả
	 └── report.csv                                                             # Báo cáo kết quả xuất ra
```

## Yêu cầu (Requirements)

- Cài đặt các thư viện cần thiết:

  ```powershell
  pip install -r requirements.txt
  ```

## Cách chạy chương trình `Cau2.py`

Hỗ trợ hai phương pháp xử lý: **pandas** hoặc **sqlite**.

```powershell
python Cau2.py [--method pandas|sqlite] [--input INPUT] [--output OUTPUT]
```

Tham số:
- `--method`: Chọn phương pháp xử lý, giá trị là `pandas` hoặc `sqlite`. Mặc định `pandas`.
- `--input`: Đường dẫn file CSV đầu vào. Mặc định `data_input/orders.csv`.
- `--output`: Đường dẫn file CSV kết quả. Mặc định `data_output/report.csv`.

Ví dụ:

```powershell
python Cau2.py
python Cau2.py --method pandas --input data_input/orders.csv --output data_output/report.csv
python Cau2.py --method sqlite --input data_input/orders.csv --output data_output/report.csv
```

Kết quả sẽ được lưu tại `data_output/report.csv` hoặc file bạn chỉ định.

## Ghi chú

- Xem chi tiết [đề bài]([Revised]%20Kyanon%20Digital%20-%20Data%20Engineer%20Intern%20-%20Entrance%20Assessment.pdf)
