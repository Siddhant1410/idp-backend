from pydantic import BaseSettings, Field
from functools import lru_cache

class Settings(BaseSettings):
    # --------------------------------------
    # FASTAPI APP SETTINGS
    # --------------------------------------
    APP_NAME: str = "IntelligentDocumentProcessing"
    APP_ENV: str = "development"
    APP_PORT: int = 8000
    APP_HOST: str = "0.0.0.0"
    DEBUG: bool = True
    SECRET_KEY: str

    # --------------------------------------
    # DATABASE SETTINGS
    # --------------------------------------
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DATABASE_URL: str

    # --------------------------------------
    # AIRFLOW SETTINGS
    # --------------------------------------
    AIRFLOW__CORE__EXECUTOR: str
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: str
    AIRFLOW__CORE__FERNET_KEY: str
    AIRFLOW__WEBSERVER__SECRET_KEY: str
    AIRFLOW__WEBSERVER__BASE_URL: str

    # --------------------------------------
    # NIFI SETTINGS
    # --------------------------------------
    NIFI_API_URL: str
    NIFI_INGEST_FLOW_ID: str
    NIFI_API_USER: str
    NIFI_API_PASSWORD: str

    # --------------------------------------
    # AWS SETTINGS
    # --------------------------------------
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str
    S3_BUCKET_NAME: str

    # --------------------------------------
    # GENAI / OPENAI SETTINGS
    # --------------------------------------
    OPENAI_API_KEY: str
    OPENAI_MODEL: str = "gpt-4o"
    OPENAI_TIMEOUT: int = 60

    # --------------------------------------
    # MISC SETTINGS
    # --------------------------------------
    LOG_LEVEL: str = "info"
    DOCUMENT_POLL_INTERVAL: int = 30

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Cached instance to avoid re-reading on each import
@lru_cache()
def get_settings():
    return Settings()
