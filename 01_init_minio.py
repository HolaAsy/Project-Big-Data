"""Langkah 1: buat bucket MinIO + aktifkan versioning pada bucket data mentah."""
from minio import Minio
from minio.commonconfig import ENABLED
from minio.versioningconfig import VersioningConfig

import config as C


def main() -> None:
    client = Minio(C.MINIO_ENDPOINT, access_key=C.MINIO_ACCESS_KEY,
                   secret_key=C.MINIO_SECRET_KEY, secure=C.MINIO_SECURE)

    for bucket in (C.BUCKET_RAW, C.BUCKET_PROCESSED):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"[+] bucket dibuat   : {bucket}")
        else:
            print(f"[=] bucket sudah ada: {bucket}")

    # Data mentah = sumber kebenaran. Versioning melindungi dari tertimpa/terhapus.
    client.set_bucket_versioning(C.BUCKET_RAW, VersioningConfig(ENABLED))
    print(f"[+] versioning aktif: {C.BUCKET_RAW}")


if __name__ == "__main__":
    main()
