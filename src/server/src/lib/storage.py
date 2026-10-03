import hashlib
import uuid
from functools import cache

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import Depends
from sqlalchemy.orm import Session

from environment import AWS_ACCESS_KEY_ID, AWS_ENDPOINT_URL_S3, AWS_REGION, AWS_SECRET_ACCESS_KEY
from lib.database import get_database
from lib.models import Asset

BUCKET = "main"


@cache
def _get_storage():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID.get_secret_value(),
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY.get_secret_value(),
        endpoint_url=AWS_ENDPOINT_URL_S3.get_secret_value(),
        region_name=AWS_REGION.get_secret_value(),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def upload_file(file_data: bytes, filename: str, content_type: str, db: Session = Depends(get_database)) -> Asset:
    asset_id = uuid.uuid4()
    file_hash = hashlib.sha256(file_data).hexdigest()
    _get_storage().put_object(Bucket=BUCKET, Key=str(asset_id), Body=file_data, ContentType=content_type)

    asset = Asset(
        id=asset_id,
        name=filename,
        size=len(file_data),
        type=content_type,
        hash=file_hash,
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


def download_file(asset_id: uuid.UUID, db: Session = Depends(get_database)) -> bytes | None:
    if db.query(Asset).filter(Asset.id == asset_id).first() is None:
        return None

    try:
        response = _get_storage().get_object(Bucket=BUCKET, Key=str(asset_id))
        with response["Body"] as body:
            return body.read()
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return None
        raise


def get_file(asset_id: uuid.UUID, db: Session = Depends(get_database)) -> Asset | None:
    return db.query(Asset).filter(Asset.id == asset_id).first()


def delete_file(asset_id: uuid.UUID, db: Session = Depends(get_database)) -> bool:
    asset = db.query(Asset).filter(Asset.id == asset_id).first()
    if asset is None:
        return False

    _get_storage().delete_object(Bucket=BUCKET, Key=str(asset_id))

    db.delete(asset)
    db.commit()
    return True
