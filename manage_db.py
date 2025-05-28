import os
import sys
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import app.models  # Import models to ensure they are registered correctly
from app.db.base import Base
from app.utils.config import settings
from app.db.session import SessionLocal
from app.crud.role import (
    create_role, delete_role, get_all_roles, get_role, get_role_by_name, update_role,
    create_permission, delete_permission, get_all_permissions, get_permission, update_permission,
    add_permission_to_role, remove_permission_from_role
)
from app.crud.user import create_user, get_user_by_username, assign_role_to_user
from app.utils.security import get_password_hash
import psycopg2
from psycopg2 import sql

# Database connection URL
DATABASE_URL = settings.DATABASE_URL

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Database connection settings from environment variables
DATABASES = {
    'default': {
        'ENGINE': 'postgresql',
        'NAME': os.getenv('DB_NAME', 'db'),
        'USER': os.getenv('DB_USER', 'username'),
        'DEFAULT_USER': os.getenv('DEFAULT_POSTGRES_USER', 'postgres'),
        'DEFAULT_PASSWORD': os.getenv('DEFAULT_POSTGRES_PASSWORD', 'postgres'),
        'PASSWORD': os.getenv('DB_PASSWORD', 'password'),
        'HOST': os.getenv('DB_HOST', 'localhost'),
        'PORT': os.getenv('DB_PORT', '5432'),
    }
}

def get_db_url():
    db = DATABASES['default']
    return f"postgresql://{db['USER']}:{db['PASSWORD']}@{db['HOST']}:{db['PORT']}/{db['NAME']}"

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_postgres_user():
    db = DATABASES['default']
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user=db['DEFAULT_USER'],
            password=db['DEFAULT_PASSWORD'],
            host=db['HOST'],
            port=db['PORT']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create the database user if it does not exist
        cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s;").format(
            sql.Identifier(db['USER'])
        ), [db['PASSWORD']])
        
        conn.close()
        print(f"User '{db['USER']}' created successfully.")
    except psycopg2.errors.DuplicateObject:
        print(f"User '{db['USER']}' already exists.")
    except OperationalError as e:
        print(f"Error: {e}")
        print(f"Failed to create user '{db['USER']}'.")
        sys.exit(1)
    except Exception as e:
        print(f"Failed to create user '{db['USER']}': {e}")

def drop_postgres_user():
    db = DATABASES['default']
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user=db['DEFAULT_USER'],
            password=db['DEFAULT_PASSWORD'],
            host=db['HOST'],
            port=db['PORT']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Drop the database user if it exists
        cursor.execute(sql.SQL("DROP USER IF EXISTS {};").format(
            sql.Identifier(db['USER'])
        ))
        
        conn.close()
        print(f"User '{db['USER']}' dropped successfully.")
    except OperationalError as e:
        print(f"Error: {e}")
        print(f"Failed to drop user '{db['USER']}'.")
        sys.exit(1)
    except Exception as e:
        print(f"Failed to drop user '{db['USER']}']: {e}")

def create_database():
    db = DATABASES['default']
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user=db['DEFAULT_USER'],
            password=db['DEFAULT_PASSWORD'],
            host=db['HOST'],
            port=db['PORT']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create the database if it does not exist
        cursor.execute(sql.SQL("CREATE DATABASE {} OWNER {};").format(
            sql.Identifier(db['NAME']),
            sql.Identifier(db['USER'])
        ))
        
        cursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {};").format(
            sql.Identifier(db['NAME']),
            sql.Identifier(db['USER'])
        ))
        
        conn.close()
        print(f"Database '{db['NAME']}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{db['NAME']}' already exists.")
    except OperationalError as e:
        print(f"Error: {e}")
        print(f"Failed to create database '{db['NAME']}'.")
        sys.exit(1)
    except Exception as e:
        print(f"Failed to create database '{db['NAME']}']: {e}")

def drop_database():
    db = DATABASES['default']
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user=db['USER'],
            password=db['PASSWORD'],
            host=db['HOST'],
            port=db['PORT']
        )
        conn.autocommit = True

        cursor = conn.cursor()
        
        # Terminate all connections to the target database before dropping it
        cursor.execute(sql.SQL("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s;
        """), [db['NAME']])

        cursor.execute(sql.SQL("DROP DATABASE {};").format(
            sql.Identifier(db['NAME'])
        ))
        
        conn.close()
        print("Database dropped successfully.")
    except OperationalError as e:
        print(f"Error: {e}")
        print(f"Failed to drop database {db['NAME']}.")
    except Exception as e:
        print(f"Failed to drop database {db['NAME']}: {e}")

def create_tables():
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully.")

def drop_tables():
    try:
        Base.metadata.drop_all(bind=engine)
        print("Database tables dropped successfully.")
    except Exception as e:
        print(f"Failed to drop tables: {e}")

def reset_database():
    try:
        drop_tables()
        create_tables()
    except Exception as e:
        print(f"Failed to reset database: {e}")

def check_table_exists(table_name: str):
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    return table_name in tables

def list_tables():
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("Tables in the database:")
    for table in tables:
        print(f" - {table}")

def list_columns(table_name: str):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    print(f"Columns in table '{table_name}':")
    for column in columns:
        print(f" - {column['name']}: {column['type']}")

def truncate_tables(tables: list):
    try:
        with engine.connect() as conn:
            for table in tables:
                conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;"))
        print("Tables truncated successfully.")
    except Exception as e:
        print(f"Failed to truncate tables: {e}")

def backup_database(backup_file: str):
    try:
        with engine.connect() as conn:
            with open(backup_file, 'w') as file:
                for line in conn.execution_options(stream_results=True).connection.connection.iterdump():
                    file.write('%s\n' % line)
        print(f"Database backed up successfully to {backup_file}.")
    except Exception as e:
        print(f"Failed to backup database: {e}")

def restore_database(backup_file: str):
    try:
        with engine.connect() as conn:
            with open(backup_file, 'r') as file:
                sql = file.read()
            conn.execute(text(sql))
        print(f"Database restored successfully from {backup_file}.")
    except Exception as e:
        print(f"Failed to restore database: {e}")

def create_user(db: SessionLocal, username: str, email: str, password: str, name: str, role: str = "user"):
    from app.schemas.user import UserCreate
    from app.crud.user import create_user as create_user_crud
    from app.utils.security import get_password_hash

    user_create = UserCreate(
        username=username,
        email=email,
        name=name,
        password=get_password_hash(password),  # Ensure password is hashed
        role=role
    )
    try:
        user = create_user_crud(db, user_create)
        print(f"User '{username}' created successfully")
    except Exception as e:
        print(f"Failed to create user: {e}")

def create_roles_and_permissions(db: SessionLocal):
    from app.schemas.user import RoleCreate, PermissionCreate
    from app.models.user import Role, Permission

    # Create admin role
    admin_role = RoleCreate(name='admin')
    try:
        db_admin_role = create_role(db, admin_role)
        print(f"Admin role '{admin_role.name}' created successfully.")
    except Exception as e:
        db_admin_role = get_role(db, admin_role.name)
        print(f"Admin role '{admin_role.name}' already exists.")

    # Define permissions
    permissions = [
        "create_role", "read_role", "update_role", "delete_role",
        "read_all_roles", "create_permission", "read_permission", 
        "update_permission", "delete_permission", "read_all_permissions",
        "add_permission_to_role", "remove_permission_from_role",
        "create_user", "read_user", "update_user", "delete_user", 
        "read_all_users", "assign_role_to_user", "remove_role_from_user", 
        "generate_token"
    ]

    # Create permissions and assign to admin role
    for perm in permissions:
        db_perm = PermissionCreate(name=perm)
        try:
            db_permission = create_permission(db, db_perm)
            print(f"Permission '{perm}' created successfully.")
        except Exception as e:
            db_permission = get_permission(db, perm)
            print(f"Permission '{perm}' already exists.")
        add_permission_to_role(db, db_admin_role, db_permission)

def check_user_exists(username):
    with next(get_db()) as db:
        user = get_user_by_username(db, username)
        if user:
            print(f"User '{username}' exists in the database with hashed password: {user.password}")
        else:
            print(f"User '{username}' does not exist in the database.")




if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Database management script")
    parser.add_argument("--create-pg-user", action="store_true", help="Create the PostgreSQL user")
    parser.add_argument("--drop-pg-user", action="store_true", help="Drop the PostgreSQL user")

    parser.add_argument("--create-db", action="store_true", help="Create the database")
    parser.add_argument("--drop-db", action="store_true", help="Drop the database")
    parser.add_argument("--create-tables", action="store_true", help="Create database tables")
    parser.add_argument("--reset-db", action="store_true", help="Reset the database (drop and create tables)")
    parser.add_argument("--check-table", type=str, help="Check if a table exists")
    parser.add_argument("--list-tables", action="store_true", help="List all tables in the database")
    parser.add_argument("--list-columns", type=str, help="List all columns in a specified table")
    parser.add_argument("--truncate-tables", nargs='+', help="Truncate specified tables")
    parser.add_argument("--backup-db", type=str, help="Backup the database to a specified file")
    parser.add_argument("--restore-db", type=str, help="Restore the database from a specified file")
    parser.add_argument("--create-user", action="store_true", help="Create a user. Add --username, --email, --password and --fullname as well.")
    parser.add_argument("--username", type=str, help="Username for the user")
    parser.add_argument("--email", type=str, help="Email for the user")
    parser.add_argument("--password", type=str, help="Password for the user")
    parser.add_argument("--fullname", type=str, help="Full name for the user")
    parser.add_argument("--role", type=str, default="user", help="Role for the user (default: user)")

    parser.add_argument("--create-role", type=str, help="Create a role")
    parser.add_argument("--delete-role", type=str, help="Delete a role")

    parser.add_argument("--create-permission", type=str, help="Create a permission")
    parser.add_argument("--delete-permission", type=str, help="Delete a permission")
    
    parser.add_argument("--assign-role", action="store_true", help="Assign a role to a user. Add --username, --rolename")
    parser.add_argument("--rolename", type=str, help="Name of the role to assign to the user")

    parser.add_argument("--create-roles-and-permissions", action="store_true", help="Add endpoint permissions to database")
    
    args = parser.parse_args()

    if args.create_pg_user:
        create_postgres_user()
    if args.drop_pg_user:
        drop_postgres_user()
    if args.create_db:
        create_database()
    if args.drop_db:
        drop_database()
    if args.create_tables:
        create_tables()
    if args.reset_db:
        reset_database()
    if args.check_table:
        exists = check_table_exists(args.check_table)
        print(f"Table '{args.check_table}' exists: {exists}")
    if args.list_tables:
        list_tables()
    if args.list_columns:
        list_columns(args.list_columns)
    if args.truncate_tables:
        truncate_tables(args.truncate_tables)
    if args.backup_db:
        backup_database(args.backup_db)
    if args.restore_db:
        restore_database(args.restore_db)
    if args.create_user:
        if not (args.username and args.email and args.password and args.fullname):
            print("Username, email, password, and full name must be provided to create a user.")
            sys.exit(1)
        with next(get_db()) as db:
            create_user(db, args.username, args.email, args.password, args.fullname, args.role)
    if args.create_role:
        with next(get_db()) as db:
            role_data = RoleCreate(name=args.create_role)
            create_role(db, role_data)
            print(f"Role '{args.create_role}' created successfully.")
    if args.delete_role:
        with next(get_db()) as db:
            if delete_role(db, args.delete_role):
                print(f"Role '{args.delete_role}' deleted successfully.")
            else:
                print(f"Role '{args.delete_role}' does not exist.")
    if args.create_permission:
        with next(get_db()) as db:
            permission_data = PermissionCreate(name=args.create_permission)
            create_permission(db, permission_data)
            print(f"Permission '{args.create_permission}' created successfully.")
    if args.delete_permission:
        with next(get_db()) as db:
            if delete_permission(db, args.delete_permission):
                print(f"Permission '{args.delete_permission}' deleted successfully.")
            else:
                print(f"Permission '{args.delete_permission}' does not exist.")
    if args.assign_role:
        if not (args.username and args.rolename):
            print("Username and rolename must be provided to assign a role to a user.")
            sys.exit(1)
        with next(get_db()) as db:
            user = get_user_by_username(db, args.username)
            if not user:
                print(f"User '{args.username}' does not exist.")
                sys.exit(1)
            role = get_role_by_name(db, args.rolename)
            if not role:
                print(f"Role '{args.rolename}' does not exist.")
                sys.exit(1)
            assign_role_to_user(db, user, role)
            print(f"Role '{args.rolename}' assigned to user '{args.username}' successfully.")
    if args.create_roles_and_permissions:
        with next(get_db()) as db:
            create_roles_and_permissions(db)

