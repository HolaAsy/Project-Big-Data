"""Langkah 2: buat database dan tabel ClickHouse.

Per skala (1x/10x/50x/100x) dibuat dua tabel dengan skema identik:
  - orders_<skala>    : MergeTree, data disimpan & dikompres di ClickHouse
  - orders_s3_<skala> : engine S3, data tetap di MinIO (dibaca langsung)
"""
import sys

import clickhouse_connect

import config as C

COLS = ",\n    ".join(f"{name} {typ}" for name, typ in C.SCHEMA.items())


def ddl_mergetree(scale: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {C.CH_DATABASE}.{C.table_name(scale)}
(
    {COLS}
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(waktu_pesanan_dibuat)
ORDER BY (provinsi, status_pesanan, waktu_pesanan_dibuat)
SETTINGS index_granularity = 8192
"""


def ddl_s3(scale: str) -> str:
    url = f"{C.MINIO_INTERNAL_URL}/{C.BUCKET_PROCESSED}/{C.processed_prefix(scale)}*.parquet"
    return f"""
CREATE TABLE IF NOT EXISTS {C.CH_DATABASE}.{C.s3_table_name(scale)}
(
    {COLS}
)
ENGINE = S3('{url}', '{C.MINIO_ACCESS_KEY}', '{C.MINIO_SECRET_KEY}', 'Parquet')
"""


def main() -> None:
    client = clickhouse_connect.get_client(host=C.CH_HOST, port=C.CH_PORT,
                                           username=C.CH_USER, password=C.CH_PASSWORD)
    if "--reset" in sys.argv:                       # hapus semua tabel lama lalu buat ulang
        client.command(f"DROP DATABASE IF EXISTS {C.CH_DATABASE}")
        print(f"[!] database {C.CH_DATABASE} dihapus (reset)")
    client.command(f"CREATE DATABASE IF NOT EXISTS {C.CH_DATABASE}")
    for scale in C.SCALES:
        client.command(ddl_mergetree(scale))
        client.command(ddl_s3(scale))
        print(f"[+] tabel siap: {C.table_name(scale)}, {C.s3_table_name(scale)}")


if __name__ == "__main__":
    main()