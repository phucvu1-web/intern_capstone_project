# ELT Pipeline

Một pipeline ELT hoàn toàn chạy local, được container hóa — nhận dữ liệu thô từ CSV vào PostgreSQL, làm sạch và biến đổi bằng dbt, và điều phối toàn bộ quy trình bằng Apache Airflow — tất cả chạy qua Docker Compose, không phụ thuộc cloud.

**Luồng xử lý:**

```
File CSV → PostgreSQL (raw) → dbt staging models → dbt mart model → Bảng báo cáo
```

Toàn bộ được điều phối bởi Airflow DAG theo thứ tự:

```
create_tables → ingest_data → run_dbt_transformations
```

---

## Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────┐
│                  Docker Network (pipeline_net)               │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │   Airflow    │    │  PostgreSQL  │    │  dbt (ephemeral│  │
│  │  Webserver   │───▶│  Warehouse   │◀───│   container)  │  │
│  │  + Scheduler │    │  :5432       │    │               │  │
│  │  :8080       │    └──────────────┘    └───────────────┘  │
│  └──────────────┘                                            │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │  Airflow DB  │    │   pgAdmin    │    │   Metabase    │  │
│  │  (Postgres)  │    │   :5050      │    │   :3000       │  │
│  └──────────────┘    └──────────────┘    └───────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Công nghệ sử dụng

| Công nghệ | Phiên bản | Vai trò |
|-----------|-----------|---------|
| Docker + Docker Compose | latest | Khởi chạy và quản lý container |
| PostgreSQL | 15-alpine | Lưu trữ dữ liệu thô và kết quả biến đổi |
| Apache Airflow | 2.7.1 (Python 3.11) | Điều phối workflow |
| dbt-core | 1.11.11 | Biến đổi dữ liệu bằng SQL |
| dbt-postgres | 1.10.0 | Adapter kết nối dbt với PostgreSQL |
| Python (pandas, sqlalchemy) | 3.11 | Script nạp dữ liệu |
| pgAdmin | latest | Giao diện quản lý database |
| Metabase | v0.61.x | Trực quan hóa dữ liệu (stretch goal) |

---

## Cấu trúc thư mục

```
intern_capstone_project/
├── dags/
|   |__ elt_dag.py  #  schedule_daily
│   └── elt_pipeline_dag.py         # Định nghĩa Airflow DAG
├── scripts/
│   ├── create_tables.py            # Tạo bảng raw trong PostgreSQL
│   └── ingest.py                   # Nạp dữ liệu CSV vào PostgreSQL
├── dbt_project/
│   ├── dbt_project.yml             # Cấu hình dbt project
│   ├── profiles.yml                # Cấu hình kết nối dbt
│   ├── dbt-requirements.txt        # Thư viện dbt
│   ├── models/
│   │   ├── staging/
│   │   │   ├── schema.yml          # Khai báo sources + test staging
│   │   │   ├── stg_pets.sql        # Làm sạch raw_pets
│   │   │   ├── stg_owners.sql      # Làm sạch raw_owners
│   │   │   └── stg_visits.sql      # Làm sạch raw_visits
│   │   └── mart/
│   │       ├── schema.yml          # Test mart model
│   │       └── mart_pet_report.sql # Bảng báo cáo tổng hợp cuối
│   └── seeds/
│       ├── pets.csv
│       ├── owners.csv
│       └── visits.csv
├── data/                           # File CSV nguồn
│   ├── pets.csv
│   ├── owners.csv
│   └── visits.csv
├── Dockerfile.dbt                  # Image dbt
├── docker-compose.yml
└── requirements.txt
```

---

## Yêu cầu hệ thống

- [Docker](https://docs.docker.com/get-docker/) và Docker Compose đã được cài đặt
- Docker daemon đang chạy
- Các cổng sau phải còn trống:

| Cổng | Dịch vụ |
|------|---------|
| `8080` | Airflow UI |
| `5050` | pgAdmin |
| `5434` | PostgreSQL (từ host) |
| `3000` | Metabase |

---

## Hướng dẫn cài đặt

### Bước 1 — Clone repository

```bash
git clone https://github.com/<your-username>/intern_capstone_project.git
cd intern_capstone_project
```

### Bước 2 — Build và khởi động tất cả dịch vụ

```bash
docker-compose up --build -d
```

Lệnh này sẽ khởi động: PostgreSQL warehouse, Airflow (webserver + scheduler + init), pgAdmin, dbt, và Metabase.

Chờ khoảng 30 giây để Airflow khởi tạo xong, sau đó kiểm tra trạng thái:

```bash
docker-compose ps
```

### Bước 3 — Truy cập các dịch vụ

| Dịch vụ | Địa chỉ | Tài khoản |
|---------|---------|-----------|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| pgAdmin | http://localhost:5050 | `admin@gmail.com` / `admin` |
| Metabase | http://localhost:3000 

### Bước 4 — Kết nối pgAdmin với warehouse

Trong pgAdmin, thêm server mới với thông tin:

| Trường | Giá trị |
|--------|---------|
| Host | `postgres_warehouse` |
| Port | `5432` |
| Database | `postgres_warehousedb` |
| Username | `postgres_warehouse` |
| Password | `postgres_warehouse` |

## Chạy pipeline

### Kích hoạt DAG

Mở Airflow UI tại http://localhost:8080
Tien hanh chay dag: elt_pipeline_dag

### Ý nghĩa từng task

| Task | Operator | Hành động |
|------|----------|-----------|
| `create_tables` | PythonOperator | Drop và tạo lại các bảng `raw_pets`, `raw_owners`, `raw_visits` |
| `ingest_data` | PythonOperator | Truncate và nạp lại từng bảng raw từ file CSV nguồn |
| `run_dbt_transformations` | DockerOperator | Khởi động container dbt, chạy `dbt run` rồi `dbt test` |
---

## Biến đổi dữ liệu

### Các vấn đề chất lượng dữ liệu được xử lý

| Vấn đề | Cột | Cách xử lý |
|--------|-----|------------|
| Giá trị boolean không nhất quán (`Y`, `Yes`, `yes`, `N`, `No`) | `vaccinated` | Chuẩn hóa thành `Yes` / `No` / `Unknown` |
| Định dạng ngày hỗn hợp (`2024/03/10`, `15-04-2024`) | `visit_date` | Phân tích bằng CASE với regex → kiểu `DATE` |
| Chữ hoa/thường không nhất quán (`cat`, `Cat`, `CAT`) | `species`, `pet_name` | Áp dụng `INITCAP(LOWER(...))` |
| Chuỗi rỗng được coi là hợp lệ | nhiều cột | Thay bằng `NULL` qua `NULLIF(TRIM(...), '')` |
| Thiếu giá trị số | `age`, `cost` | Mặc định là `0` qua `COALESCE` |
| Thiếu giá trị văn bản | `email`, `phone`, `city`, `vet_name`, `notes` | Mặc định là `'N/A'` hoặc `'Unknown'` |

### Sơ đồ lineage dbt

```
raw_pets   ──▶ stg_pets   ──┐
raw_owners ──▶ stg_owners ──┼──▶ mart_pet_report
raw_visits ──▶ stg_visits ──┘
```

### Bảng báo cáo cuối: `mart_pet_report`

| Cột | Kiểu dữ liệu | Mô tả |
|-----|-------------|-------|
| `pet_id` | INTEGER | Mã định danh thú cưng |
| `pet_name` | TEXT | Tên thú cưng (đã chuẩn hóa chữ hoa/thường) |
| `species` | TEXT | Loài (Dog / Cat) |
| `age` | INTEGER | Tuổi (0 nếu không rõ) |
| `vaccinated` | TEXT | Trạng thái tiêm phòng: Yes / No / Unknown |
| `full_name` | TEXT | Họ tên chủ nuôi |
| `email` | TEXT | Email chủ nuôi (N/A nếu thiếu) |
| `phone` | TEXT | Số điện thoại (N/A nếu thiếu) |
| `city` | TEXT | Thành phố (Unknown nếu thiếu) |
| `total_visits` | INTEGER | Tổng số lượt khám |
| `total_cost` | NUMERIC | Tổng chi phí tất cả các lượt khám |
| `first_visit` | DATE | Ngày khám đầu tiên |
| `last_visit` | DATE | Ngày khám gần nhất |
| `visit_reasons` | TEXT | Danh sách lý do khám (phân cách bằng dấu phẩy) |

---

## Kiểm thử dbt

Các test được định nghĩa trong file `schema.yml` và tự động chạy trong task `run_dbt_transformations`.

| Model | Test | Cột |
|-------|------|-----|
| `stg_pets` | `not_null`, `unique` | `pet_id` |
| `stg_pets` | `not_null` | `species`, `owner_id` |
| `stg_pets` | `accepted_values` (`Yes`,`No`,`Unknown`) | `vaccinated` |
| `stg_owners` | `not_null`, `unique` | `owner_id` |
| `stg_owners` | `not_null` | `full_name` |
| `stg_visits` | `not_null`, `unique` | `visit_id` |
| `stg_visits` | `not_null` | `pet_id` |
| `mart_pet_report` | `not_null`, `unique` | `pet_id` |
| `mart_pet_report` | `not_null` | `pet_name`, `full_name` |
| `mart_pet_report` | `accepted_values` (`Yes`,`No`,`Unknown`) | `vaccinated` |

---

## Xác nhận kết quả

Sau khi DAG chạy thành công, kiểm tra bảng kết quả bằng psql:

```bash
# Kết nối từ ngoài Docker
psql -h localhost -p 5434 -U postgres_warehouse -d postgres_warehousedb
```

```sql
-- Xem toàn bộ bảng báo cáo
SELECT * FROM mart.mart_pet_report ORDER BY pet_id;

-- Kiểm tra số lượng bản ghi
SELECT COUNT(*) FROM mart.mart_pet_report;

-- Kiểm tra không có NULL ở các cột bắt buộc
SELECT *
FROM mart.mart_pet_report
WHERE pet_name IS NULL
   OR full_name IS NULL
   OR vaccinated IS NULL;
```
## Dừng hệ thống

```bash
# Dừng và xóa container
docker-compose down