# Use Debian 12 (Bookworm) slim image
FROM python:3.11-slim-bookworm

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose the internal Flask port
EXPOSE 8004

# Run the application with threads to handle concurrent SSE streams
CMD ["gunicorn", "--workers", "4", "--threads", "8", "--worker-class", "gthread", "--bind", "0.0.0.0:8004", "--timeout", "900", "app:app"]
