# -----------------------------------------
# Base image
# -----------------------------------------
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system-level dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from root context (adjusted for new build context)
COPY ./requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy FastAPI app and config modules
COPY ./apps /app/apps
COPY ./core /app/core

# Expose FastAPI port
EXPOSE 8000

# Run FastAPI app using Uvicorn
CMD ["uvicorn", "apps.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
