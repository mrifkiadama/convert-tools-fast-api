from fastapi import APIRouter
from app.api.v1.endpoints import auth
from app.api.v1.endpoints.user_management import users, roles
from app.api.v1.endpoints import convert_tool

api_router = APIRouter()
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(users.router, prefix="/users", tags=["User Management"])
api_router.include_router(roles.router, prefix="/roles_permissions", tags=["Roles and Permissions"])


api_router.include_router(convert_tool.router, prefix="/convert_tools", tags=["Convert Tools"])
