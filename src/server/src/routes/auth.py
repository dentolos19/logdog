from __future__ import annotations

from datetime import datetime, timedelta, timezone
import uuid

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from environment import SECRET_KEY
from lib.database import get_database
from lib.models import User

ACCESS_TOKEN_EXPIRES_MINUTES = 30
REFRESH_TOKEN_EXPIRES_DAYS = 7
JWT_ALGORITHM = "HS256"

router = APIRouter(prefix="/auth", tags=["auth"])

bearer_scheme = HTTPBearer(auto_error=False)


class RegisterRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class MessageResponse(BaseModel):
    message: str


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    email: str
    created_at: datetime


def hash_password(password: str):
    if not password:
        raise ValueError("Password must not be empty.")

    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain_password: str, hashed_password: str):
    if not plain_password or not hashed_password:
        return False

    try:
        return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))
    except ValueError:
        return False


def _create_token(subject: str, token_type: str, expires_delta: timedelta):
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "type": token_type,
        "iat": int(now.timestamp()),
        "exp": int((now + expires_delta).timestamp()),
    }
    return jwt.encode(payload, SECRET_KEY.get_secret_value(), algorithm=JWT_ALGORITHM)


def create_access_token(subject: str):
    return _create_token(
        subject=subject,
        token_type="access",
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRES_MINUTES),
    )


def create_refresh_token(subject: str):
    return _create_token(
        subject=subject,
        token_type="refresh",
        expires_delta=timedelta(days=REFRESH_TOKEN_EXPIRES_DAYS),
    )


def decode_token(token: str, expected_type: str):
    try:
        payload = jwt.decode(token, SECRET_KEY.get_secret_value(), algorithms=[JWT_ALGORITHM])
    except JWTError as error:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.") from error

    token_type = str(payload.get("type") or "")
    if token_type != expected_type:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type.")

    subject = str(payload.get("sub") or "")
    if not subject:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token subject is missing.")

    return subject


def _normalize_email(email: str):
    return email.strip().lower()


def _uuid_or_raw(value: str):
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return value


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    database: Session = Depends(get_database),
):
    if credentials is None or not credentials.credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing authorization token.")

    user_id = decode_token(credentials.credentials, expected_type="access")
    user = database.query(User).filter(User.id == _uuid_or_raw(user_id)).first()
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found.")

    return user


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register(payload: RegisterRequest, database: Session = Depends(get_database)):
    normalized_email = _normalize_email(payload.email)
    if not normalized_email:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Email must not be empty.")
    if not payload.password:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Password must not be empty.")

    existing_user = database.query(User).filter(User.email == normalized_email).first()
    if existing_user is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email is already registered.")

    user = User(email=normalized_email, password=hash_password(payload.password))
    database.add(user)
    database.commit()
    database.refresh(user)

    return UserResponse(id=str(user.id), email=user.email, created_at=user.created_at)


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, database: Session = Depends(get_database)):
    normalized_email = _normalize_email(payload.email)
    user = database.query(User).filter(User.email == normalized_email).first()
    if user is None or not verify_password(payload.password, user.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")

    user_id = str(user.id)
    return TokenResponse(
        access_token=create_access_token(subject=user_id),
        refresh_token=create_refresh_token(subject=user_id),
    )


@router.post("/refresh", response_model=TokenResponse)
def refresh(payload: RefreshRequest, database: Session = Depends(get_database)):
    user_id = decode_token(payload.refresh_token, expected_type="refresh")
    user = database.query(User).filter(User.id == _uuid_or_raw(user_id)).first()
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found.")

    normalized_user_id = str(user.id)
    return TokenResponse(
        access_token=create_access_token(subject=normalized_user_id),
        refresh_token=create_refresh_token(subject=normalized_user_id),
    )


@router.post("/logout", response_model=MessageResponse)
def logout():
    return MessageResponse(message="Logged out.")


@router.get("/me", response_model=UserResponse)
def me(current_user: User = Depends(get_current_user)):
    return UserResponse(id=str(current_user.id), email=current_user.email, created_at=current_user.created_at)
