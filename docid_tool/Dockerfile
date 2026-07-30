# Use Debian 12 (Bookworm) slim image
FROM python:3.11-slim-bookworm

# Set working directory
WORKDIR /app

# Unbuffered stdout: without this Python block-buffers the pipe under gunicorn,
# so error logs can sit unflushed and `docker logs -f docid-tool` shows nothing
# while the service is failing.
ENV PYTHONUNBUFFERED=1

# Install system dependencies and Microsoft ODBC Driver 18 for SQL Server
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose the internal Flask port
EXPOSE 8002

# Checks the cache, not just the process. The failure this service actually had
# was "process up, every API request 500ing" — a check against / could not see
# it, because / renders fine with the API completely broken.
HEALTHCHECK --interval=60s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8002/health || exit 1

# Run the application
CMD ["gunicorn", "--workers", "4", "--bind", "0.0.0.0:8002", "--timeout", "120", "--access-logfile", "-", "--error-logfile", "-", "app:app"]