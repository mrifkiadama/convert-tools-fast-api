from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional, List
from datetime import datetime
from app.models.user import Permission

class UserBase(BaseModel):
    username: str
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    roles: Optional[List[str]] = None

class UserCreate(UserBase):
    password: str  # Plain text password

class UserUpdate(UserBase):
    password: Optional[str] = None

class UserResponse(UserBase):
    id: int
    created_at: datetime
    last_login: Optional[datetime] = None
    roles: List[str] = []

    class Config:
        from_attributes = True

class User(UserBase):
    id: int
    password: str
    created_at: datetime
    last_login: Optional[datetime] = None
    roles: List[str] = []

    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class RoleBase(BaseModel):
    name: str

class RoleCreate(RoleBase):
    pass

class RoleUpdate(RoleBase):
    pass

class Role(RoleBase):
    id: int
    permissions: List[str]

    class Config:
        from_attributes = True

    @validator('permissions', pre=True, each_item=True)
    def convert_permissions_to_strings(cls, perm):
        if isinstance(perm, Permission):
            return perm.name
        return perm

class PermissionBase(BaseModel):
    name: str

class PermissionCreate(PermissionBase):
    pass

class PermissionUpdate(PermissionBase):
    pass

class PermissionResponse(PermissionBase):
    id: int

    class Config:
        from_attributes = True