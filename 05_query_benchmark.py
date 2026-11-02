"""Langkah 5: benchmark lengkap sisi MinIO + ClickHouse sesuai rencana uji tim
(menggabungkan kueri analitik + semua metrik: tulis, baca, muat, kueri, CPU, RAM, success rate).

Memenuhi 8 metrik rencana uji, untuk skenario S1-S4 (1x, 10x, 50x, 100x):

  Write/Ingestion Time : (a) unggah Parquet ke MinIO, (b) muat MinIO -> tabel ClickHouse
  Read Time            : membaca file Parquet dari MinIO sampai habis
  Query Execution Time : 4 kueri analitik (lihat QUERIES di bawah), 1 dingin + sisanya hangat
  Throughput           : MB/detik dan baris/detik (dihitung otomatis)
  Storage size         : ukuran di ClickHouse (terkompresi) dan Parquet di MinIO
  CPU Usage            : rata-rata dan maksimum (via `docker stats`, % dari 1 core, bisa > 100)
  Memory Usage         : puncak RAM container minio + clickhouse (MB)
  Success rate         : % operasi/kueri yang berhasil dari seluruh percobaan

Jalankan dari folder proyek (venv aktif, docker compose up -d sudah jalan):
  python 05_query_benchmark.py
  python 05_query_benchmark.py --repeat 5 --also-s3     # lebih teliti, plus kueri langsung ke MinIO
  python 05_query_benchmark.py --scales 1x 10x          # uji cepat dulu
  python 05_query_benchmark.py --no-cpu                 # tanpa CPU/RAM: waktu paling murni

Keluaran: results/benchmark_detail.csv (semua percobaan) dan results/benchmark_ringkasan.csv.

CATATAN KETEPATAN CPU/RAM: `docker stats` butuh 1-2 detik per pembacaan, jadi untuk fase yang
lebih singkat dari ~5 detik nilai CPU/RAM-nya kasar atau kosong. Nilai yang paling bisa
diandalkan ada pada fase panjang (muat 50x/100x dan blok kueri). Jelaskan ini di laporan.
"""
import argparse
import contextlib
import re
import statistics
import subprocess
import threading
import time
from pathlib import Path

import config as C


# 4 kueri analitik (harus identik dengan yang dijalankan Performance Analyst di jalur HDFS)
QUERIES = {
    "Q1_pesanan_per_provinsi": """
        SELECT provinsi, count() AS pesanan, sum(total_pembayaran) AS total_bayar
        FROM {t} GROUP BY provinsi ORDER BY total_bayar DESC""",
    "Q2_status_pesanan": """
        SELECT status_pesanan, count() AS pesanan, avg(total_pembayaran) AS rata_bayar
        FROM {t} GROUP BY status_pesanan ORDER BY pesanan DESC""",
    "Q3_metode_pembayaran": """
        SELECT metode_pembayaran, count() AS pesanan, sum(total_pembayaran) AS total_bayar
        FROM {t} GROUP BY metode_pembayaran ORDER BY total_bayar DESC""",
    "Q4_tren_bulanan_antarwilayah": """
        SELECT toStartOfMonth(waktu_pesanan_dibuat) AS bulan, provinsi,
               count() AS pesanan, sum(total_pembayaran) AS total_bayar
        FROM {t} GROUP BY bulan, provinsi ORDER BY bulan, total_bayar DESC""",
}

CONTAINERS = ("minio", "clickhouse")
_UNIT = {"b": 1, "kb": 1e3, "mb": 1e6, "gb": 1e9, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3}


# ----------------------------------------------------------------------------------
# Pengukur CPU / RAM lewat `docker stats`
# ----------------------------------------------------------------------------------
def parse_stats_line(line: str):
    """'clickhouse|12.5%|512.3MiB / 2.9GiB' -> ('clickhouse', 12.5, 537.2)  (cpu %, mem MB)."""
    parts = line.strip().split("|")
    if len(parts) != 3:
        return None
    name, cpu, mem = parts
    try:
        cpu_val = float(cpu.strip().rstrip("%"))
    except ValueError:
        return None
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", mem)
    if not m:
        return None
    mem_mb = float(m.group(1)) * _UNIT.get(m.group(2).lower(), 1) / 1e6
    return name.strip(), cpu_val, mem_mb


class Sampler:
    """Context manager: mengambil sampel CPU/RAM container selama blok `with` berjalan."""

    def __init__(self):
        self.samples = []          # (cpu_total, mem_total_mb)
        self.available = True
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        while not self._stop.is_set():
            try:
                out = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format",
                     "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"],
                    capture_output=True, text=True, timeout=30)
            except Exception:
                self.available = False
                return
            if out.returncode != 0:
                self.available = False
                return
            cpu = mem = 0.0
            seen = 0
            for line in out.stdout.splitlines():
                p = parse_stats_line(line)
                if p and p[0] in CONTAINERS:
                    cpu += p[1]
                    mem += p[2]
                    seen += 1
            if seen:
                self.samples.append((cpu, mem))

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=10)

    def summary(self):
        """(cpu_rata2, cpu_maks, ram_maks_mb) atau (None, None, None) bila tidak ada sampel."""
        if not self.samples:
            return None, None, None
        cpus = [s[0] for s in self.samples]
        mems = [s[1] for s in self.samples]
        return round(statistics.mean(cpus), 1), round(max(cpus), 1), round(max(mems), 1)


# ----------------------------------------------------------------------------------
# Pencatatan satu percobaan
# ----------------------------------------------------------------------------------
def run_phase(records, scale, phase, fn, *, rows=None, mb=None, repeat=0, run_type="",
              verify=None, sample=True, tag=""):
    """Jalankan fn() sambil mengukur waktu (+CPU/RAM bila sample=True). Gagal = dicatat, bukan crash."""
    error = ""
    ok = True
    sampler = Sampler() if sample else None
    if sampler:
        sampler.__enter__()
    t0 = time.perf_counter()
    try:
        fn()
    except Exception as e:                       # noqa: BLE001 - kegagalan memang dicatat
        ok = False
        error = f"{type(e).__name__}: {e}"[:200]
    seconds = time.perf_counter() - t0
    if sampler:
        sampler.__exit__(None, None, None)
    if ok and verify is not None:                # verifikasi di luar hitungan waktu
        try:
            verify()
        except Exception as e:                   # noqa: BLE001
            ok = False
            error = f"verifikasi: {e}"[:200]
    cpu_avg, cpu_max, mem_max = sampler.summary() if sampler else (None, None, None)
    rec = {"scale": scale, "repeat": repeat, "phase": phase, "run_type": run_type,
           "seconds": round(seconds, 4), "rows": rows, "mb": mb,
           "rows_per_s": round(rows / seconds, 1) if rows and seconds > 0 else None,
           "mb_per_s": round(mb / seconds, 2) if mb and seconds > 0 else None,
           "success": ok, "cpu_avg_pct": cpu_avg, "cpu_max_pct": cpu_max,
           "mem_max_mb": mem_max, "error": error,
           "tag": tag, "ch_cpu_pct": None, "ch_mem_mb": None}
    records.append(rec)
    flag = "OK " if ok else "GAGAL"
    print(f"  [{flag}] {scale:>4} {phase:<34} {seconds:9.3f} s  {error}")
    return rec


# ----------------------------------------------------------------------------------
# CPU/RAM per kueri dari system.query_log ClickHouse (tidak mengganggu waktu ukur)
# ----------------------------------------------------------------------------------
def cpu_mem_from_log_row(duration_ms, user_us, sys_us, mem_bytes):
    """CPU % = (waktu CPU user+sys) / (durasi kueri). 100% = 1 core penuh, bisa > 100 (multi-core)."""
    cpu = None
    if duration_ms and duration_ms > 0:
        cpu = round((user_us + sys_us) / (duration_ms * 1000.0) * 100, 1)
    return cpu, round(mem_bytes / 1e6, 1)


def attach_ch_metrics(records, log_rows):
    """Isi ch_cpu_pct / ch_mem_mb pada records berdasarkan tag (log_comment).
    log_rows: iterable (tag, duration_ms, user_us, sys_us, mem_bytes)."""
    by_tag = {r[0]: cpu_mem_from_log_row(*r[1:]) for r in log_rows}
    n = 0
    for rec in records:
        if rec.get("tag") in by_tag:
            rec["ch_cpu_pct"], rec["ch_mem_mb"] = by_tag[rec["tag"]]
            n += 1
    return n


def fetch_log_rows(ch, run_id):
    ch.command("SYSTEM FLUSH LOGS")
    res = ch.query(
        "SELECT log_comment, query_duration_ms, ProfileEvents['UserTimeMicroseconds'], "
        "ProfileEvents['SystemTimeMicroseconds'], memory_usage "
        "FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE %(p)s",
        parameters={"p": f"{run_id}|%"})
    return res.result_rows


# ----------------------------------------------------------------------------------
# Ringkasan (murni pandas, mudah diuji)
# ----------------------------------------------------------------------------------
def summarize(df):
    import pandas as pd

    ok = df[df["success"]]
    keys = ["scale", "phase", "run_type"]
    g_all = df.groupby(keys, dropna=False)
    out = g_all.agg(percobaan=("success", "size"),
                    berhasil=("success", "sum")).reset_index()
    out["success_rate_pct"] = (out["berhasil"] / out["percobaan"] * 100).round(1)
    g_ok = ok.groupby(keys, dropna=False)
    med = g_ok.agg(detik_median=("seconds", "median"),
                   mb_per_s_median=("mb_per_s", "median"),
                   rows_per_s_median=("rows_per_s", "median"),
                   cpu_avg_pct=("cpu_avg_pct", "mean"),
                   cpu_max_pct=("cpu_max_pct", "max"),
                   mem_max_mb=("mem_max_mb", "max"),
                   ch_cpu_avg_pct=("ch_cpu_pct", "mean"),
                   ch_cpu_max_pct=("ch_cpu_pct", "max"),
                   ch_mem_max_mb=("ch_mem_mb", "max")).reset_index()
    res = out.merge(med, on=keys, how="left")
    scale_order = {s: i for i, s in enumerate(C.SCALES)}
    res["_o"] = res["scale"].map(scale_order)
    res = res.sort_values(["_o", "phase", "run_type"]).drop(columns="_o")
    return res.round(3)


# ----------------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeat", type=int, default=3, help="ulangan untuk tulis/baca/muat")
    ap.add_argument("--runs", type=int, default=6, help="eksekusi per kueri (1 dingin + sisanya hangat)")
    ap.add_argument("--dir", type=Path, default=Path("data/output/skenario"))
    ap.add_argument("--scales", nargs="*", default=C.SCALES)
    ap.add_argument("--also-s3", action="store_true", help="ukur juga kueri langsung ke Parquet di MinIO")
    ap.add_argument("--no-cpu", action="store_true",
                    help="matikan pengambilan CPU/RAM (docker stats) agar waktu tidak terganggu")
    a = ap.parse_args()
    use_cpu = not a.no_cpu

    import clickhouse_connect
    import pandas as pd
    import pyarrow.parquet as pq
    from minio import Minio

    ch = clickhouse_connect.get_client(host=C.CH_HOST, port=C.CH_PORT,
                                       username=C.CH_USER, password=C.CH_PASSWORD)
    mc = Minio(C.MINIO_ENDPOINT, access_key=C.MINIO_ACCESS_KEY,
               secret_key=C.MINIO_SECRET_KEY, secure=C.MINIO_SECURE)

    probe = Sampler()
    if use_cpu:
        with probe:
            pass
    if use_cpu and not probe.available:
        print("PERINGATAN: `docker stats` tidak bisa dipakai, kolom CPU/RAM akan kosong.\n")

    records, storage = [], []
    run_id = "bench" + time.strftime("%Y%m%d%H%M%S")   # penanda unik jalan ini di query_log
    sources = [("mergetree", C.table_name)]
    if a.also_s3:
        sources.append(("s3", C.s3_table_name))

    for scale in a.scales:
        path = a.dir / f"dataset_{scale}.parquet"
        if not path.exists():
            raise SystemExit(f"File tidak ditemukan: {path}")
        expected = pq.ParquetFile(path).metadata.num_rows
        size_mb = path.stat().st_size / 1e6
        key = f"{C.processed_prefix(scale)}part-0000.parquet"
        tbl = f"{C.CH_DATABASE}.{C.table_name(scale)}"
        src = f"{C.CH_DATABASE}.{C.s3_table_name(scale)}"
        print(f"\n=== Skenario {scale}: {expected:,} baris, {size_mb:.1f} MB Parquet ===")

        for rep in range(1, a.repeat + 1):
            # (a) Write: unggah file ke MinIO
            run_phase(records, scale, "write_minio",
                      lambda: mc.fput_object(C.BUCKET_PROCESSED, key, str(path)),
                      rows=expected, mb=size_mb, repeat=rep, sample=use_cpu)

            # (b) Read: baca objek dari MinIO sampai habis
            def read_all():
                resp = mc.get_object(C.BUCKET_PROCESSED, key)
                n = 0
                try:
                    for chunk in resp.stream(1024 * 1024):
                        n += len(chunk)
                finally:
                    resp.close()
                    resp.release_conn()
                if n != path.stat().st_size:
                    raise RuntimeError(f"ukuran terbaca {n} != {path.stat().st_size}")
            run_phase(records, scale, "read_minio", read_all,
                      rows=expected, mb=size_mb, repeat=rep, sample=use_cpu)

            # (c) Ingestion: MinIO -> tabel MergeTree
            ch.command(f"TRUNCATE TABLE {tbl}")

            def check_rows():
                n = ch.query(f"SELECT count() FROM {tbl}").first_row[0]
                if n != expected:
                    raise RuntimeError(f"baris {n} != {expected}")
            tag_ing = f"{run_id}|{scale}|ingest_clickhouse|{rep}"
            run_phase(records, scale, "ingest_clickhouse",
                      lambda: ch.command(f"INSERT INTO {tbl} SELECT * FROM {src}",
                                         settings={"log_comment": tag_ing}),
                      rows=expected, mb=size_mb, repeat=rep, verify=check_rows,
                      sample=use_cpu, tag=tag_ing)

        # Ukuran penyimpanan (setelah part digabung)
        ch.command(f"OPTIMIZE TABLE {tbl} FINAL")
        comp, uncomp = ch.query(
            "SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes) FROM system.parts "
            "WHERE database=%(d)s AND table=%(t)s AND active",
            parameters={"d": C.CH_DATABASE, "t": C.table_name(scale)}).first_row
        storage.append({"scale": scale, "baris": expected,
                        "clickhouse_terkompresi_mb": round(comp / 1e6, 2),
                        "clickhouse_tak_terkompresi_mb": round(uncomp / 1e6, 2),
                        "parquet_minio_mb": round(size_mb, 2)})

        # Kueri: satu blok bersampel CPU/RAM per sumber
        for source_name, namer in sources:
            table = f"{C.CH_DATABASE}.{namer(scale)}"
            block = Sampler() if use_cpu else None
            t_block = time.perf_counter()
            with (block if use_cpu else contextlib.nullcontext()):
                for qname, sql in QUERIES.items():
                    for i in range(a.runs):
                        tag_q = f"{run_id}|{scale}|query_{source_name}:{qname}|{i}"
                        run_phase(records, scale, f"query_{source_name}:{qname}",
                                  lambda s=sql, tg=tag_q: ch.query(
                                      s.format(t=table),
                                      settings={"use_query_cache": 0, "log_comment": tg}),
                                  rows=expected, repeat=i,
                                  run_type="dingin" if i == 0 else "hangat", sample=False,
                                  tag=tag_q)
            cpu_avg, cpu_max, mem_max = block.summary() if use_cpu else (None, None, None)
            records.append({"scale": scale, "repeat": 0, "phase": f"query_blok_{source_name}",
                            "run_type": "", "seconds": round(time.perf_counter() - t_block, 3),
                            "rows": None, "mb": None, "rows_per_s": None, "mb_per_s": None,
                            "success": True, "cpu_avg_pct": cpu_avg, "cpu_max_pct": cpu_max,
                            "mem_max_mb": mem_max, "error": "",
                            "tag": "", "ch_cpu_pct": None, "ch_mem_mb": None})

    # CPU/RAM ClickHouse per operasi (ingest + kueri) dari system.query_log
    try:
        n = attach_ch_metrics(records, fetch_log_rows(ch, run_id))
        print(f"\nCPU/RAM ClickHouse terisi dari query_log untuk {n} operasi.")
    except Exception as e:                           # noqa: BLE001
        print(f"\nPERINGATAN: gagal membaca system.query_log ({e}); kolom ch_* kosong.")

    Path("results").mkdir(exist_ok=True)
    df = pd.DataFrame(records).drop(columns="tag")
    df.to_csv("results/benchmark_detail.csv", index=False)
    summary = summarize(df)
    summary.to_csv("results/benchmark_ringkasan.csv", index=False)
    pd.DataFrame(storage).to_csv("results/benchmark_storage.csv", index=False)

    print("\n=== SELESAI ===")
    print(f"Success rate keseluruhan: {df['success'].mean() * 100:.1f}% "
          f"({int(df['success'].sum())}/{len(df)} operasi)")
    print("Tersimpan: results/benchmark_detail.csv, benchmark_ringkasan.csv, benchmark_storage.csv")


if __name__ == "__main__":
    main()