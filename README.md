# MinIO + ClickHouse Performance Benchmark

## 1. Overview

Project ini bertujuan untuk menguji dan membandingkan performa **MinIO sebagai Object Storage** dan **ClickHouse sebagai Analytical Database** menggunakan dataset e-commerce dalam format Parquet.

Pengujian dilakukan menggunakan empat skala dataset, yaitu 1x, 10x, 50x, dan 100x.

Aspek yang diuji meliputi:
- Write dan read performance.
- Data ingestion.
- Query execution time.
- Throughput.
- Penggunaan CPU dan RAM.
- Storage efficiency.
- Success rate.

## 2. Architecture

```text
       E-Commerce Dataset
               |
               v
        Parquet Files
               |
               v
          MinIO Storage
               |
          +----+----+
          |         |
          v         v
      S3 Engine  MergeTree      (ClickHouse)
          |         ^
          |         |
          +---------+
               |
               v
       Benchmark Testing
               |
               v
         Results (CSV)
```

Terdapat dua pendekatan penyimpanan:

- **MinIO + S3 Engine:** ClickHouse membaca data secara langsung dari MinIO.
- **ClickHouse MergeTree:** Data dari MinIO dimuat terlebih dahulu ke ClickHouse untuk kebutuhan analitik.

## 3. Technology Stack

| Technology | Function |
|---|---|
| Python | Automation dan benchmark |
| Docker Compose | Infrastructure |
| MinIO | Object Storage |
| ClickHouse | Analytical Database |
| Apache Parquet | Data Format |
| Pandas | Data Processing |
| PyArrow | Parquet Processing |

## 4. Project Structure

```text
project/
│
├── docker-compose.yml
├── requirements.txt
├── config.py
│
├── 01_init_minio.py
├── 02_init_clickhouse.py
├── 03_upload_ready.py
├── 04_load_clickhouse.py
├── 05_query_benchmark.py
│
├── data/
│   └── output/
│       └── skenario/
│           ├── dataset_1x.parquet
│           ├── dataset_10x.parquet
│           ├── dataset_50x.parquet
│           └── dataset_100x.parquet
│
└── results/
    ├── load_metrics.csv
    ├── benchmark_detail.csv
    ├── benchmark_ringkasan.csv
    └── benchmark_storage.csv
```

## 5. Installation

### Requirements

- Python 3.10+
- Docker dan Docker Compose
- Dataset Parquet hasil preprocessing

### Install Dependencies

```bash
python -m venv venv
```

Aktifkan virtual environment:

Windows:
```bash
venv\Scripts\activate
```

Linux/macOS:
```bash
source venv/bin/activate
```

Install library:

```bash
pip install -r requirements.txt
```

## 6. Configuration

Konfigurasi project berada di `config.py`.

Konfigurasi utama:

| Parameter | Value |
|---|---|
| MinIO Endpoint | localhost:9000 |
| MinIO Console | localhost:9001 |
| ClickHouse HTTP | localhost:8123 |
| ClickHouse Native | localhost:9009 |
| Database | ecommerce |
| Raw Bucket | ecom-raw |
| Processed Bucket | ecom-processed |
| Dataset Scales | 1x, 10x, 50x, 100x |

## 7. Execution

Jalankan setiap script sesuai urutan berikut:

```bash
# 1. Menjalankan MinIO dan ClickHouse
docker compose up -d

# 2. Inisialisasi bucket MinIO
python 01_init_minio.py

# 3. Inisialisasi database dan tabel ClickHouse
python 02_init_clickhouse.py

# 4. Upload dataset Parquet ke MinIO
python 03_upload_ready.py

# 5. Ingestion data ke ClickHouse MergeTree
python 04_load_clickhouse.py

# 6. Menjalankan benchmark
python 05_query_benchmark.py
```

Opsi tambahan benchmark:

```bash
# Mengatur jumlah pengulangan
python 05_query_benchmark.py --repeat 5

# Mengaktifkan benchmark langsung melalui S3 Engine
python 05_query_benchmark.py --also-s3

# Mengatur jumlah eksekusi query
python 05_query_benchmark.py --runs 10

# Menjalankan benchmark pada skala tertentu
python 05_query_benchmark.py --scales 1x 10x

# Menonaktifkan monitoring CPU/RAM
python 05_query_benchmark.py --no-cpu
```

## 8. Benchmark Results

Hasil pengujian disimpan dalam folder `results/`.

Terdapat empat file CSV yang memiliki fungsi berbeda agar hasil pengujian lebih mudah dibaca, dianalisis, dan dibandingkan.

### 8.1. load_metrics.csv

**Data hasil ingestion ke ClickHouse**

File ini berisi informasi hasil proses pemindahan data dari MinIO ke tabel ClickHouse MergeTree.

Informasi yang dicatat meliputi:

- Skala dataset.
- Jumlah baris yang berhasil dimuat.
- Waktu yang dibutuhkan untuk proses ingestion.
- Ukuran data terkompresi di ClickHouse.
- Ukuran data sebelum kompresi di ClickHouse.
- Ukuran file Parquet di MinIO.

**Fungsi:** Mengukur waktu proses loading data serta membandingkan ukuran penyimpanan setelah data dimuat ke ClickHouse dengan ukuran file Parquet di MinIO.

File ini dihasilkan oleh `04_load_clickhouse.py`.

### 8.2. benchmark_detail.csv

**Data pengujian detail**

File ini berisi hasil pengujian benchmark secara terperinci untuk setiap percobaan.

Informasi yang dicatat meliputi:

- Skala dataset: 1x, 10x, 50x, dan 100x.
- Jenis pengujian: MinIO write, MinIO read, ClickHouse ingestion, dan query.
- Nomor pengulangan dan tingkat keberhasilan.
- Waktu eksekusi dan throughput.
- Jumlah baris yang diproses per detik.
- Penggunaan CPU dan RAM.
- Penggunaan CPU dan RAM ClickHouse jika tersedia.
- Informasi error jika pengujian gagal.

**Fungsi:** Mengetahui hasil setiap pengujian secara detail dan menganalisis performa sistem pada setiap percobaan.

### 8.3. benchmark_ringkasan.csv

**Data hasil rangkuman**

File ini berisi hasil benchmark yang telah dirangkum dari data pengujian detail.

Informasi yang ditampilkan meliputi:

- Median waktu eksekusi.
- Median throughput.
- Rata-rata dan maksimum penggunaan CPU.
- Penggunaan RAM maksimum.
- Success rate.
- Perbandingan performa berdasarkan skala dataset.
- Ringkasan cold query dan warm query.

**Fungsi:** Mempermudah perbandingan performa MinIO dan ClickHouse tanpa harus membaca seluruh data pengujian secara detail.

File ini menjadi sumber utama untuk membuat tabel dan grafik perbandingan performa.

### 8.4. benchmark_storage.csv

**Data ukuran penyimpanan**

File ini berisi informasi ukuran dataset dan ruang penyimpanan yang digunakan oleh MinIO dan ClickHouse.

Informasi yang dicatat meliputi:

- Skala dataset.
- Jumlah baris.
- Ukuran data terkompresi ClickHouse.
- Ukuran data sebelum kompresi ClickHouse.
- Ukuran file Parquet di MinIO.

**Fungsi:** Membandingkan efisiensi penyimpanan antara ClickHouse dan MinIO pada setiap skala dataset.

## 9. Why Are the Results Divided into Four CSV Files?

Pembagian hasil benchmark dilakukan agar setiap file memiliki tujuan analisis yang berbeda.

| File | Main Purpose |
|---|---|
| `load_metrics.csv` | Mengukur waktu ingestion dan mencatat ukuran data setelah loading |
| `benchmark_detail.csv` | Melihat hasil setiap percobaan |
| `benchmark_ringkasan.csv` | Membandingkan performa sistem |
| `benchmark_storage.csv` | Menganalisis ukuran penyimpanan |

## 10. Cleanup

Menghentikan sementara container tanpa menghapus container:

```bash
docker compose stop
```

Menjalankan kembali container yang sudah dihentikan:

```bash
docker compose start
```

Menghentikan dan menghapus container:

```bash
docker compose down
```

Menghapus container sekaligus seluruh persistent volumes:

```bash
docker compose down -v
```

**Perhatian:**
- `docker compose stop`: Menghentikan container, tetapi container dan data tetap tersimpan.
- `docker compose start`: Menjalankan kembali container yang sudah dihentikan.
- `docker compose down`: Menghentikan dan menghapus container, tetapi persistent volumes tetap tersimpan.
- `docker compose down -v`: Menghapus container sekaligus persistent volumes, termasuk data MinIO dan ClickHouse.
