# Dockerfile for Electra FastAPI backend
# Builds a minimal image, installs requirements and runs uvicorn

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
     && apt-get install -y --no-install-recommends \
         build-essential \
         gfortran \
         libopenblas-dev \
         liblapack-dev \
         libblas-dev \
     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

# Ensure we run from the electra-app subfolder where main.py lives
WORKDIR /app/electra-app

# Create a non-root user and give ownership of the app directory
RUN useradd -m appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Start uvicorn (change workers or options for production as needed)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
