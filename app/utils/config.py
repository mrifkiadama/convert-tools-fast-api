import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    SECRET_KEY: str = os.getenv('SECRET_KEY', 'your_secret_key')
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    DATABASE_URL: str = os.getenv('DATABASE_URL', 'postgresql://username:password@localhost:5432/test')
    FILE_UPLOAD_DIR: str = os.getenv('FILE_UPLOAD_DIR', './files')

    class Config:
        case_sensitive = True

settings = Settings()