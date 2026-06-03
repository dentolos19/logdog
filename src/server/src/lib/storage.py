import hashlib
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastapi import Depends
from sqlalchemy.orm import Session

from environment import STORAGE_URL
from lib.database import get_database
from lib.models import Asset


def _request(method: str, asset_id: uuid.UUID, data: bytes | None = None, content_type: str | None = None) -> bytes:
    headers = {"Content-Type": content_type} if content_type else {}
    request = Request(f"{STORAGE_URL.get_secret_value()}/{asset_id}", data=data, headers=headers, method=method)
    with urlopen(request, timeout=30) as response:
        return response.read()


def upload_file(file_data: bytes, filename: str, content_type: str, db: Session = Depends(get_database)) -> Asset:
    asset_id = uuid.uuid4()
    file_hash = hashlib.sha256(file_data).hexdigest()
    _request("PUT", asset_id, file_data, content_type)

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
        return _request("GET", asset_id)
    except (HTTPError, URLError):
        return None


def get_file(asset_id: uuid.UUID, db: Session = Depends(get_database)) -> Asset | None:
    return db.query(Asset).filter(Asset.id == asset_id).first()


def delete_file(asset_id: uuid.UUID, db: Session = Depends(get_database)) -> bool:
    asset = db.query(Asset).filter(Asset.id == asset_id).first()
    if asset is None:
        return False

    try:
        _request("DELETE", asset_id)
    except (HTTPError, URLError):
        pass

    db.delete(asset)
    db.commit()
    return True
