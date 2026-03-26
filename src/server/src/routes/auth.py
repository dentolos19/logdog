from fastapi import APIRouter, Depends, HTTPException, status
from lib.auth import (
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

router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class UserResponse(BaseModel):
    id: str
    email: str

    model_config = {"from_attributes": True}


class AuthSessionResponse(BaseModel):
    user: UserResponse
    access_token: str
    refresh_token: str


def _build_auth_session(user: User) -> AuthSessionResponse:
    user_id = str(user.id)
    return AuthSessionResponse(
        user=user,
        access_token=create_access_token(user_id),
        refresh_token=create_refresh_token(user_id),
    )


@router.post("/register", response_model=AuthSessionResponse, status_code=status.HTTP_201_CREATED)
def register(body: RegisterRequest, db: Session = Depends(get_db)) -> AuthSessionResponse:
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
    return _build_auth_session(user)


@router.post("/login", response_model=AuthSessionResponse)
def login(body: LoginRequest, db: Session = Depends(get_db)) -> AuthSessionResponse:
    user = db.query(User).filter(User.email == body.email).first()
    # Use a single error message for both cases to prevent user enumeration.
    if user is None or not verify_password(body.password, str(user.password)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password.",
        )
    return _build_auth_session(user)


@router.post("/refresh", response_model=AuthSessionResponse)
def refresh(body: RefreshRequest, db: Session = Depends(get_db)) -> AuthSessionResponse:
    if body.refresh_token.strip() == "":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token missing.",
        )
    user_id = decode_token(body.refresh_token, "refresh")
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found.",
        )
    return _build_auth_session(user)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout() -> None:
    return None


@router.get("/me", response_model=UserResponse)
def me(current_user: User = Depends(get_current_user)) -> User:
    return current_user
