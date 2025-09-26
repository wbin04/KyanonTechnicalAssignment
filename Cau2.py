# Bài tập kỹ thuật (Data Aggregation)

import os
import argparse
import pandas as pd
import sqlite3

def run_pandas(input_path: str, output_path: str):
    df = pd.read_csv(input_path, dtype={"order_id": str, "customer_id": str})

    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df = df[df["status"] == "completed"].copy()

    print("Danh sách các đơn hàng đã hoàn thành (pandas):")
    print(f"{df}\n")

    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    report = (
        df.groupby("order_date", as_index=False)["amount"]
          .sum()
          .rename(columns={"order_date": "date"})
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    report.to_csv(output_path, index=False)

    print("Dữ liệu tổng amount theo ngày (pandas):")
    print(report)


def run_sqlite(input_path: str, output_path: str):
    df = pd.read_csv(input_path, dtype={"order_id": str, "customer_id": str})
    con = sqlite3.connect(":memory:")

    try:
        df.to_sql("orders_raw", con, index=False, if_exists="replace")

        completed_sql = """
        SELECT
            order_id,
            order_date,
            customer_id,
            amount,
            status
        FROM orders_raw
        WHERE lower(trim(status)) = 'completed'
        ORDER BY order_date, order_id;
        """
        completed_df = pd.read_sql_query(completed_sql, con)
        print("Danh sách các đơn hàng đã hoàn thành (SQL/SQLite):")
        print(f"{completed_df}\n")

        report_sql = """
        WITH cleaned AS (
            SELECT
                DATE(order_date) AS date,
                CAST(amount AS REAL) AS amount,
                lower(trim(status)) AS status
            FROM orders_raw
        )
        SELECT
            date,
            SUM(amount) AS amount
        FROM cleaned
        WHERE status = 'completed'
        GROUP BY date
        ORDER BY date;
        """
        report_df = pd.read_sql_query(report_sql, con)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        report_df.to_csv(output_path, index=False)

        print("Dữ liệu tổng amount theo ngày (SQL/SQLite):")
        print(report_df)
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description="Data Aggregation (2 cách: pandas / sqlite)")
    parser.add_argument("--method", choices=["pandas", "sqlite"], default="pandas",
                        help="Chọn phương pháp xử lý: pandas hoặc sqlite (SQL).")
    parser.add_argument("--input", default="data_input/orders.csv",
                        help="Đường dẫn file CSV đầu vào (mặc định: data_input/orders.csv)")
    parser.add_argument("--output", default="data_output/report.csv",
                        help="Đường dẫn file CSV kết quả (mặc định: data_output/report.csv)")
    args = parser.parse_args()

    if args.method == "pandas":
        run_pandas(args.input, args.output)
    else:
        run_sqlite(args.input, args.output)


if __name__ == "__main__":
    print("Câu 2: Bài tập kỹ thuật (Data Aggregation)\n")
    main()
