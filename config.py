"""Konfigurasi bersama untuk seluruh skrip (MinIO + ClickHouse, data e-commerce).

Ubah lewat environment variable atau edit nilai default di bawah.
Catatan: default ini hanya untuk lab lokal/Docker. Jangan dipakai di produksi.
"""
import os
import re

# --------------------------------------------------------------------------
# MinIO
# --------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")      # dari host (Python)
MINIO_INTERNAL_URL = os.getenv("MINIO_INTERNAL_URL", "http://minio:9000")  # dari container ClickHouse
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

BUCKET_RAW = "ecom-raw"              # CSV asli, tidak boleh diubah (immutable)
BUCKET_PROCESSED = "ecom-processed"  # Parquet bersih per skala

# Tata letak objek (Hive-style partition: mudah dibaca ClickHouse, Spark, Trino)
#   ecom-raw/source=kaggle/ingest_date=YYYY-MM-DD/<nama_file>.csv
#   ecom-processed/scale=1x/part-0000.parquet
SCALES = ["1x", "10x", "50x", "100x"]


def raw_prefix(ingest_date: str) -> str:
    return f"source=kaggle/ingest_date={ingest_date}/"


def processed_prefix(scale: str) -> str:
    return f"scale={scale}/"


# --------------------------------------------------------------------------
# ClickHouse
# --------------------------------------------------------------------------
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))   # HTTP
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "ch_pass123")
CH_DATABASE = "ecommerce"

# --------------------------------------------------------------------------
# Skema target (20 kolom, mengikuti Parquet dari Data Collection Engineer)
# nama kolom -> tipe ClickHouse
# --------------------------------------------------------------------------
SCHEMA = {
    "order_id": "String",
    "total_qty": "Int64",
    "total_weight_gr": "Int64",
    "total_returned_qty": "Int64",
    "total_diskon": "Int64",
    "product_categories": "String",
    "num_product_categories": "Int64",
    "status_pesanan": "LowCardinality(String)",
    "alasan_pembatalan": "LowCardinality(String)",
    "opsi_pengiriman": "LowCardinality(String)",
    "metode_pembayaran": "LowCardinality(String)",
    "kota_kabupaten": "LowCardinality(String)",
    "provinsi": "LowCardinality(String)",
    "ongkos_kirim_dibayar_oleh_pembeli": "Int64",
    "estimasi_potongan_biaya_pengiriman": "Int64",
    "total_pembayaran": "Int64",
    "perkiraan_ongkos_kirim": "Int64",
    "waktu_pesanan_dibuat": "DateTime64(6)",
    "source_file": "LowCardinality(String)",
    "waktu_imputasi": "Int8",
}


def normalize(name: str) -> str:
    """'Total Pembayaran' -> 'total_pembayaran' (snake_case, huruf kecil)."""
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower())
    return s.strip("_")


# Peta nama kolom hasil normalize() dari CSV Kaggle -> nama kolom target.
# SESUAIKAN dengan header asli dataset (cek df.columns). Kolom yang namanya
# sudah sama dengan SCHEMA tidak perlu dimasukkan.
COLUMN_MAP = {
    # "total_diskon": "total_discount",
    # "status_pesanan": "order_status",
    # "alasan_pembatalan": "cancel_reason",
    # "opsi_pengiriman": "shipping_option",
    # "metode_pembayaran": "payment_method",
    # "kota_kabupaten": "city",
    # "provinsi": "province",
    # "ongkos_kirim_dibayar_oleh_pembeli": "shipping_fee_paid_by_buyer",
    # "estimasi_potongan_biaya_pengiriman": "est_shipping_fee_discount",
    # "total_pembayaran": "total_payment",
    # "perkiraan_ongkos_kirim": "est_shipping_fee",
    # "waktu_pesanan_dibuat": "order_created_at",
}


def table_name(scale: str) -> str:
    return f"orders_{scale}"          # tabel MergeTree (data di dalam ClickHouse)


def s3_table_name(scale: str) -> str:
    return f"orders_s3_{scale}"       # tabel S3 (data tetap di MinIO)