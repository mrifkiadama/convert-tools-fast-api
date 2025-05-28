from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.schemas.user import RoleCreate, RoleUpdate, Role, PermissionCreate, PermissionUpdate, PermissionResponse
from app.crud.role import (
    create_role, get_role, update_role, delete_role, get_all_roles,
    create_permission, get_permission, update_permission, delete_permission, get_all_permissions,
    add_permission_to_role, remove_permission_from_role
)
from app.utils.dependencies import get_db, permission_required

router = APIRouter()

# Role Endpoints
@router.post("/roles/", response_model=Role, status_code=201, dependencies=[Depends(permission_required("create_role"))])
def create_new_role(role: RoleCreate, db: Session = Depends(get_db)):
    return create_role(db=db, role=role)

@router.get("/roles/{role_id}", response_model=Role, dependencies=[Depends(permission_required("read_role"))])
def read_role(role_id: int, db: Session = Depends(get_db)):
    role = get_role(db, role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    return role

@router.put("/roles/{role_id}", response_model=Role, dependencies=[Depends(permission_required("update_role"))])
def update_role_endpoint(role_id: int, role: RoleUpdate, db: Session = Depends(get_db)):
    updated_role = update_role(db, role_id, role)
    if not updated_role:
        raise HTTPException(status_code=404, detail="Role not found")
    return updated_role

@router.delete("/roles/{role_id}", status_code=204, dependencies=[Depends(permission_required("delete_role"))])
def delete_role_endpoint(role_id: int, db: Session = Depends(get_db)):
    if not delete_role(db, role_id):
        raise HTTPException(status_code=404, detail="Role not found")

@router.get("/roles/", response_model=list[Role], dependencies=[Depends(permission_required("read_all_roles"))])
def read_roles(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return get_all_roles(db, skip, limit)

# Permission Endpoints
@router.post("/permissions/", response_model=PermissionResponse, status_code=201, dependencies=[Depends(permission_required("create_permission"))])
def create_new_permission(permission: PermissionCreate, db: Session = Depends(get_db)):
    return create_permission(db=db, permission=permission)

@router.get("/permissions/{permission_id}", response_model=PermissionResponse, dependencies=[Depends(permission_required("read_permission"))])
def read_permission(permission_id: int, db: Session = Depends(get_db)):
    permission = get_permission(db, permission_id)
    if not permission:
        raise HTTPException(status_code=404, detail="Permission not found")
    return permission

@router.put("/permissions/{permission_id}", response_model=PermissionResponse, dependencies=[Depends(permission_required("update_permission"))])
def update_permission_endpoint(permission_id: int, permission: PermissionUpdate, db: Session = Depends(get_db)):
    updated_permission = update_permission(db, permission_id, permission)
    if not updated_permission:
        raise HTTPException(status_code=404, detail="Permission not found")
    return updated_permission

@router.delete("/permissions/{permission_id}", status_code=204, dependencies=[Depends(permission_required("delete_permission"))])
def delete_permission_endpoint(permission_id: int, db: Session = Depends(get_db)):
    if not delete_permission(db, permission_id):
        raise HTTPException(status_code=404, detail="Permission not found")

@router.get("/permissions/", response_model=list[PermissionResponse], dependencies=[Depends(permission_required("read_all_permissions"))])
def read_permissions(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return get_all_permissions(db, skip, limit)

# Role-Permission Management
@router.post("/roles/{role_id}/permissions/{permission_id}", response_model=Role, status_code=200, dependencies=[Depends(permission_required("add_permission_to_role"))])
def add_permission_to_role_endpoint(role_id: int, permission_id: int, db: Session = Depends(get_db)):
    db_role = get_role(db, role_id)
    if not db_role:
        raise HTTPException(status_code=404, detail="Role not found")
    db_permission = get_permission(db, permission_id)
    if not db_permission:
        raise HTTPException(status_code=404, detail="Permission not found")
    
    # Check if the permission is already associated with the role
    if db_permission in db_role.permissions:
        raise HTTPException(status_code=400, detail="Permission already assigned to role")
    
    return add_permission_to_role(db=db, role=db_role, permission=db_permission)

@router.delete("/roles/{role_id}/permissions/{permission_id}", response_model=Role, status_code=200, dependencies=[Depends(permission_required("remove_permission_from_role"))])
def remove_permission_from_role_endpoint(role_id: int, permission_id: int, db: Session = Depends(get_db)):
    db_role = get_role(db, role_id)
    if not db_role:
        raise HTTPException(status_code=404, detail="Role not found")
    db_permission = get_permission(db, permission_id)
    if not db_permission:
        raise HTTPException(status_code=404, detail="Permission not found")
    return remove_permission_from_role(db=db, role=db_role, permission=db_permission)