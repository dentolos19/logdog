import os

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from lib.auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_DAYS,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
    hash_password,
    verify_password,
)
from lib.database import get_db
from lib.models import User
from pydantic import BaseModel, EmailStr
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

_COOKIE_SECURE = os.getenv("APP_URL", "http").lower().startswith("https")
_COOKIE_SAMESITE = os.getenv("AUTH_COOKIE_SAMESITE", "none" if _COOKIE_SECURE else "lax").strip().lower()

if _COOKIE_SAMESITE not in {"lax", "strict", "none"}:
    _COOKIE_SAMESITE = "none" if _COOKIE_SECURE else "lax"

if _COOKIE_SAMESITE == "none" and not _COOKIE_SECURE:
    _COOKIE_SAMESITE = "lax"

router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class UserResponse(BaseModel):
    id: str
    email: str

    model_config = {"from_attributes": True}


def _set_auth_cookies(response: Response, user_id: str) -> None:
    """Set both httpOnly auth cookies on the given response."""
    response.set_cookie(
        key="access_token",
        value=create_access_token(user_id),
        httponly=True,
        samesite=_COOKIE_SAMESITE,
        secure=_COOKIE_SECURE,
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )
    response.set_cookie(
        key="refresh_token",
        value=create_refresh_token(user_id),
        httponly=True,
        samesite=_COOKIE_SAMESITE,
        secure=_COOKIE_SECURE,
        # Scope the refresh token to the refresh endpoint only to minimize exposure.
        path="/auth/refresh",
        max_age=REFRESH_TOKEN_EXPIRE_DAYS * 86_400,
    )


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register(body: RegisterRequest, response: Response, db: Session = Depends(get_db)) -> User:
    user = User(email=body.email, password=hash_password(body.password))
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An account with this email already exists.",
        )
    db.refresh(user)
    _set_auth_cookies(response, str(user.id))
    return user


@router.post("/login", response_model=UserResponse)
def login(body: LoginRequest, response: Response, db: Session = Depends(get_db)) -> User:
    user = db.query(User).filter(User.email == body.email).first()
    # Use a single error message for both cases to prevent user enumeration.
    if user is None or not verify_password(body.password, str(user.password)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password.",
        )
    _set_auth_cookies(response, str(user.id))
    return user


@router.post("/refresh", status_code=status.HTTP_204_NO_CONTENT)
def refresh(
    response: Response,
    refresh_token: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> None:
    if refresh_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token missing.",
        )
    user_id = decode_token(refresh_token, "refresh")
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found.",
        )
    # Issue a new access token only; the refresh token expiry is not reset.
    response.set_cookie(
        key="access_token",
        value=create_access_token(user_id),
        httponly=True,
        samesite=_COOKIE_SAMESITE,
        secure=_COOKIE_SECURE,
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(response: Response) -> None:
    response.delete_cookie(key="access_token")
    response.delete_cookie(key="refresh_token", path="/auth/refresh")


@router.get("/me", response_model=UserResponse)
def me(current_user: User = Depends(get_current_user)) -> User:
    return current_user
