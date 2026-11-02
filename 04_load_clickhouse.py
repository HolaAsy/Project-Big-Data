"""Langkah 4: muat Parquet dari MinIO ke tabel MergeTree + catat waktu muat & ukuran.

Hasil ditulis ke results/load_metrics.csv (untuk Performance Analyst).
"""
import csv
import time
from pathlib import Path

import clickhouse_connect
from minio import Minio

import config as C


def minio_size_bytes(m: Minio, bucket: str, prefix: str) -> int:
    return sum(o.size for o in m.list_objects(bucket, prefix=prefix, recursive=True))


def main() -> None:
    ch = clickhouse_connect.get_client(host=C.CH_HOST, port=C.CH_PORT,
                                       username=C.CH_USER, password=C.CH_PASSWORD)
    m = Minio(C.MINIO_ENDPOINT, access_key=C.MINIO_ACCESS_KEY,
              secret_key=C.MINIO_SECRET_KEY, secure=C.MINIO_SECURE)
    rows_out = []

    for scale in C.SCALES:
        tbl = f"{C.CH_DATABASE}.{C.table_name(scale)}"
        src = f"{C.CH_DATABASE}.{C.s3_table_name(scale)}"
        ch.command(f"TRUNCATE TABLE {tbl}")                  # idempoten: aman dijalankan ulang

        t0 = time.perf_counter()
        ch.command(f"INSERT INTO {tbl} SELECT * FROM {src}")
        load_s = time.perf_counter() - t0
        ch.command(f"OPTIMIZE TABLE {tbl} FINAL")            # gabung part agar ukuran adil

        n, comp, uncomp = ch.query(
            "SELECT sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes) "
            "FROM system.parts WHERE database=%(d)s AND table=%(t)s AND active",
            parameters={"d": C.CH_DATABASE, "t": C.table_name(scale)}).first_row
        parquet_b = minio_size_bytes(m, C.BUCKET_PROCESSED, C.processed_prefix(scale))

        rows_out.append({"scale": scale, "rows": n, "load_seconds": round(load_s, 3),
                         "clickhouse_compressed_bytes": comp,
                         "clickhouse_uncompressed_bytes": uncomp,
                         "minio_parquet_bytes": parquet_b})
        print(f"{scale:>4}: {n:>12,} baris | muat {load_s:7.2f}s | "
              f"CH {comp/1e6:8.1f} MB | Parquet {parquet_b/1e6:8.1f} MB")

    Path("results").mkdir(exist_ok=True)
    with open("results/load_metrics.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows_out[0].keys())
        w.writeheader()
        w.writerows(rows_out)


if __name__ == "__main__":
    main()
