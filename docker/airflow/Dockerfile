FROM apache/airflow:3.0.1-python3.10

USER airflow
RUN pip install openai==0.28 pytesseract pdf2image 
USER airflow

USER root
RUN apt-get update && apt-get install -y \
    poppler-utils \
    tesseract-ocr \
    libtesseract-dev \
    libgl1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER root
