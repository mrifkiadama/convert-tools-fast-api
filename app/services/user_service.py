from sqlalchemy.orm import Session
from app.schemas.user import UserCreate, UserUpdate
from app.crud import user
from app.utils.security import get_password_hash

def create_user_service(db: Session, user_create: UserCreate):
    # Do not set hashed_password here, handle this in the CRUD function
    db_user = user.create_user(db, user_create)
    return db_user

def update_user_service(db: Session, user_id: int, user_update: UserUpdate):
    update_data = user_update.dict(exclude_unset=True)
    if 'password' in update_data:
        update_data['password'] = get_password_hash(update_data.pop('password'))
    db_user = user.update_user(db, user_id, update_data)
    return db_user