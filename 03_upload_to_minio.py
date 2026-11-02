"""Langkah 3 (versi data siap pakai): unggah Parquet hasil preprocessing ke MinIO.

Tidak memproses ulang data, hanya mengirim file apa adanya (hemat RAM).
  data/output/skenario/dataset_<skala>.parquet -> ecom-processed/scale=<skala>/part-0000.parquet
  data/raw/all_months_clean.csv                -> ecom-raw/source=kaggle/ingest_date=<hari ini>/   (jika ada)

Jalankan dari folder proyek:  python 03_upload_ready.py
"""
from datetime import date
from pathlib import Path

from minio import Minio

import config as C

SKENARIO_DIR = Path("data/output/skenario")
RAW_FILE = Path("data/raw/all_months_clean.csv")


def main() -> None:
    c = Minio(C.MINIO_ENDPOINT, access_key=C.MINIO_ACCESS_KEY,
              secret_key=C.MINIO_SECRET_KEY, secure=C.MINIO_SECURE)

    missing = [s for s in C.SCALES if not (SKENARIO_DIR / f"dataset_{s}.parquet").exists()]
    if missing:
        raise SystemExit(f"File Parquet tidak ditemukan untuk skala: {missing} di {SKENARIO_DIR}")

    for scale in C.SCALES:
        src = SKENARIO_DIR / f"dataset_{scale}.parquet"
        key = f"{C.processed_prefix(scale)}part-0000.parquet"
        c.fput_object(C.BUCKET_PROCESSED, key, str(src),
                      content_type="application/octet-stream")
        print(f"[+] {scale:>4}: {src.stat().st_size/1e6:7.1f} MB -> {C.BUCKET_PROCESSED}/{key}")

    if RAW_FILE.exists():
        key = C.raw_prefix(date.today().isoformat()) + RAW_FILE.name
        c.fput_object(C.BUCKET_RAW, key, str(RAW_FILE), content_type="text/csv")
        print(f"[+] raw : {RAW_FILE.stat().st_size/1e6:7.1f} MB -> {C.BUCKET_RAW}/{key}")
    else:
        print(f"[=] {RAW_FILE} tidak ada, bucket {C.BUCKET_RAW} dilewati")


if __name__ == "__main__":
    main()