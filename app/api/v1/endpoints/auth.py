from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import timedelta
from app.utils.security import create_access_token, verify_password, get_password_hash
from app.schemas.user import Token, UserCreate, UserResponse
from app.crud.user import get_user, create_user, get_user_by_username
from app.utils.dependencies import get_db, permission_required
from app.utils.config import settings
import logging

router = APIRouter()

logger = logging.getLogger(__name__)

@router.post("/token", response_model=Token)
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    logger.debug(f"Login attempt for user {form_data.username}")
    user = get_user_by_username(db, form_data.username)
    if not user:
        logger.warning("User not found")
    elif not verify_password(form_data.password, user.password):
        logger.warning("Password mismatch")
    
    # Additional debug information
    logger.debug(f"User from DB: {user}")
    logger.debug(f"Form data: {form_data.username}")
    
    if not user or not verify_password(form_data.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    logger.info("Token created successfully")
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/create_user/", response_model=UserResponse)
def create_user_endpoint(user: UserCreate, db: Session = Depends(get_db)):
    db_user = get_user_by_username(db, user.username)
    if db_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    new_user = create_user(db=db, user=user)
    return UserResponse.from_orm(new_user)