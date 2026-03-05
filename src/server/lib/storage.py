import os
from pathlib import Path

import boto3
from fastapi import Depends, HTTPException, status
from lib.database import get_db
from lib.models import Asset
from sqlalchemy.orm import Session

_BUCKET: str = os.getenv("BUCKET_NAME", "")
_USE_S3: bool = bool(_BUCKET)
_LOCAL_DIR: Path = Path("./store")


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("BUCKET_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("BUCKET_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("BUCKET_SECRET_KEY"),
    )


def upload_file(raw_data: bytes, name: str, size: int, mime_type: str, db: Session = Depends(get_db)) -> Asset:
    record = Asset(name=name, size=size, type=mime_type)
    db.add(record)
    db.commit()
    db.refresh(record)

    if _USE_S3:
        _s3_client().put_object(Bucket=_BUCKET, Key=record.id, Body=raw_data)
    else:
        _LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        (_LOCAL_DIR / record.id).write_bytes(raw_data)

    return record


def get_file(file_id: str, db: Session = Depends(get_db)) -> bytes:
    record = db.get(Asset, file_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")

    if _USE_S3:
        response = _s3_client().get_object(Bucket=_BUCKET, Key=file_id)
        return response["Body"].read()
    else:
        path = _LOCAL_DIR / file_id
        if not path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")
        return path.read_bytes()


def delete_file(file_id: str, db: Session = Depends(get_db)) -> None:
    record = db.get(Asset, file_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")

    if _USE_S3:
        _s3_client().delete_object(Bucket=_BUCKET, Key=file_id)
    else:
        path = _LOCAL_DIR / file_id
        if path.exists():
            path.unlink()

    db.delete(record)
    db.commit()
