# FastAPI - JWT Authentication with Roles & Permissions API

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.95.0-green)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.3-blue)](https://www.postgresql.org/)

A comprehensive boilerplate designed to empower developers to rapidly build secure and scalable FastAPI applications with fine-grained access control. This project leverages JSON Web Tokens (JWT) for authentication and a PostgreSQL database to implement a robust Role-Based Access Control (RBAC) system.

## Key Features & Benefits

### Robust Authentication

- **JWT-based Authentication:** Securely authenticate users using JSON Web Tokens (JWT), providing a stateless and scalable authentication mechanism.
- **Bcrypt Password Hashing:** Employ industry-standard bcrypt for strong password hashing, safeguarding user credentials.
- **Token Expiration and Refresh:** Manage token lifetimes with configurable expiration times and a built-in refresh mechanism for enhanced user experience.

### Role-Based Access Control (RBAC)

- **Role Management:** Create and manage roles to define user groups within your application (e.g., admin, moderator, user).
- **Permission Management:** Define granular permissions for each role, granting or restricting access to specific API endpoints and actions (e.g., create_user, read_article, update_product).
- **Flexible Role-Permission Relationships:** Establish a many-to-many relationship between roles and permissions, enabling complex access control scenarios.
- **Endpoint-Level Authorization:** Easily enforce access control on FastAPI endpoints using the provided `role_required` and `permission_required` dependencies.

### Database Management

- **Simplified Setup:** Streamline database initialization, schema creation, and population of default roles and permissions with the `manage_db.py` script.
- TODO: **Future-Proof Migrations:** Utilize Alembic for managing database schema changes as your application evolves.
- **Data Security:** Safeguard your data with built-in backup and restore functionalities.

### CRUD Operations (Create, Read, Update, Delete)

- **User Management:** Comprehensive API endpoints for creating, retrieving, updating, and deleting user accounts.
- **Role & Permission Management:** Full control over roles and permissions via intuitive API endpoints.
- **Pydantic Models:** Ensure data integrity and validation with Pydantic models for user, role, and permission schemas.

### Frontend Integration (Flask)

- **Example Flask Application:** Included as a starting point, demonstrates how to interact with the API to manage users.
- **Easily Extendable:** Customize and expand the frontend to build a full-featured user management interface or integrate with your existing frontend framework.

### Additional Benefits

- **Logging:** Detailed logging of authentication events (login attempts, user creation, etc.) for debugging and security auditing.
- **Customizable:**  Easily tailor the boilerplate to fit the specific requirements of your project.
- **Open Source:** Freely use, modify, and distribute under the MIT License.

## Architecture

- **FastAPI:**  The high-performance web framework for building the backend API.
- **PostgreSQL:** The robust and scalable relational database for storing user data, roles, and permissions.
- **SQLAlchemy:** The Object-Relational Mapper (ORM) that simplifies database interactions.
- **PyJWT:** The library responsible for JWT token generation, validation, and decoding.
- **Passlib:** The library providing bcrypt password hashing.
- **Pydantic:** The data validation and serialization library.
- **Flask (optional):** A lightweight web framework for creating the example frontend.

## Installation, Configuration, Database Management, Running the Application, API Documentation


### Installation

1. Clone the repository:
    ```bash
    git clone REPO_URL
    cd REPO_NAME
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

### Configuration

1. Set up your `.env` file with the necessary environment variables:
    ```env
    SECRET_KEY=your_secret_key
    DATABASE_URL=postgresql://username:password@localhost:5432/db
    ```
    Replace `your_secret_key` with a strong secret key. Update the database URL with your actual credentials and database name.

2. Configure PostgreSQL:
    Refer to the `manage_db.py` script for detailed instructions:
    ```bash
    python manage_db.py -h
    ```

### Running the Application

1. Start the FastAPI application:
    ```bash
    uvicorn app.main:app --reload
    ```
    Your application should now be running on [http://127.0.0.1:8000](http://127.0.0.1:8000).

### API Documentation

FastAPI automatically generates interactive API documentation. You can access it at:
- [Swagger UI](http://127.0.0.1:8000/docs)
- [ReDoc](http://127.0.0.1:8000/redoc)

### Database Management

Use the `manage_db.py` script for database-related operations. Here are some common commands:

- Create the PostgreSQL user:
    ```bash
    python manage_db.py --create-pg-user
    ```
- Drop the PostgreSQL user:
    ```bash
    python manage_db.py --drop-pg-user
    ```
- Create the database:
    ```bash
    python manage_db.py --create-db
    ```
- Drop the database:
    ```bash
    python manage_db.py --drop-db
    ```
- Create database tables:
    ```bash
    python manage_db.py --create-tables
    ```
- Reset the database (drop and create tables):
    ```bash
    python manage_db.py --reset-db
    ```

For a complete list of available commands, run:
```bash
python manage_db.py -h
```
## Security Considerations

- **Secret Key Management:** Rotate your `SECRET_KEY` regularly and store it securely (e.g., using environment variables or a secrets manager).
- **HTTPS:** Deploy your application over HTTPS to encrypt traffic and prevent the interception of JWT tokens.
- **Rate Limiting:** Mitigate brute-force attacks by implementing rate limiting on authentication endpoints.
- **Input Validation:** Rigorously validate all user input to protect against common security vulnerabilities like SQL injection and cross-site scripting (XSS).
- **Least Privilege:** Adhere to the principle of least privilege by granting users only the permissions necessary for their roles.

