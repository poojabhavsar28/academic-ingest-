
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Copy dependency files
COPY requirements*.txt ./

# Install API dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements-api.txt

# Copy project files
COPY . .

# Expose API port
EXPOSE 8080

# Start the app through its launcher so Windows and container startup share the same entry point.
CMD ["python", "-m", "ingest.server"]
