import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.api import api_router

app = FastAPI(
    title=os.getenv("APP_NAME", "FastAPI RBAC Boilerplate"),
    version="1.0.0",
    docs_url="/docs",              # Swagger UI
    redoc_url="/redoc",            # ReDoc UI
    openapi_url="/openapi.json"    # OpenAPI JSON
)


@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Welcome to FastAPI RBAC Boilerplate"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(api_router, prefix="/api/v1")