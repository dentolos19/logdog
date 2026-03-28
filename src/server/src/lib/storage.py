import hashlib
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from fastapi import Depends, HTTPException, status
from lib.database import get_db
from lib.models import Asset
from sqlalchemy.orm import Session

_LOCAL_DIR: Path = Path("./store")


def _bucket_config() -> tuple[str, str, str, str, str]:
    endpoint_url = os.getenv("BUCKET_ENDPOINT_URL", "").strip()
    access_key = os.getenv("BUCKET_ACCESS_KEY", "").strip()
    secret_key = os.getenv("BUCKET_SECRET_KEY", "").strip()
    bucket_name = os.getenv("BUCKET_NAME", "").strip()
    bucket_prefix = os.getenv("BUCKET_PREFIX", "").strip().strip("/")
    return endpoint_url, access_key, secret_key, bucket_name, bucket_prefix


def _bucket_key(file_id: str, bucket_prefix: str) -> str:
    if not bucket_prefix:
        return file_id
    return f"{bucket_prefix}/{file_id}"


def _s3_client(endpoint_url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def upload_file(raw_data: bytes, name: str, size: int, mime_type: str, db: Session = Depends(get_db)) -> Asset:
    file_hash = hashlib.sha256(raw_data).hexdigest()
    record = Asset(name=name, size=size, type=mime_type, hash=file_hash)
    db.add(record)
    db.commit()
    db.refresh(record)

    endpoint_url, access_key, secret_key, bucket_name, bucket_prefix = _bucket_config()
    use_s3 = bool(endpoint_url and access_key and secret_key and bucket_name)

    if use_s3:
        key = _bucket_key(record.id, bucket_prefix)
        _s3_client(endpoint_url, access_key, secret_key).put_object(Bucket=bucket_name, Key=key, Body=raw_data)
    else:
        _LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        (_LOCAL_DIR / record.id).write_bytes(raw_data)

    return record


def get_file(file_id: str, db: Session = Depends(get_db)) -> bytes:
    record = db.get(Asset, file_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")

    endpoint_url, access_key, secret_key, bucket_name, bucket_prefix = _bucket_config()
    use_s3 = bool(endpoint_url and access_key and secret_key and bucket_name)

    if use_s3:
        try:
            key = _bucket_key(file_id, bucket_prefix)
            response = _s3_client(endpoint_url, access_key, secret_key).get_object(Bucket=bucket_name, Key=key)
            return response["Body"].read()
        except ClientError as error:
            error_code = str(error.response.get("Error", {}).get("Code", ""))
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.") from error
            raise
    else:
        path = _LOCAL_DIR / file_id
        if not path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")
        return path.read_bytes()


def delete_file(file_id: str, db: Session = Depends(get_db)) -> None:
    record = db.get(Asset, file_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")

    endpoint_url, access_key, secret_key, bucket_name, bucket_prefix = _bucket_config()
    use_s3 = bool(endpoint_url and access_key and secret_key and bucket_name)

    if use_s3:
        key = _bucket_key(file_id, bucket_prefix)
        _s3_client(endpoint_url, access_key, secret_key).delete_object(Bucket=bucket_name, Key=key)
    else:
        path = _LOCAL_DIR / file_id
        if path.exists():
            path.unlink()

    db.delete(record)
    db.commit()
