# Media dispatcher pulls from BigQuery (not SQL Server), so unlike the sales
# dispatcher we do NOT need the Microsoft ODBC driver. Keeps the image small.
FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8002

# Threads so the SSE stream and the polling requests don't block each other.
# Long timeout because a full Power BI export can take a few minutes.
CMD ["gunicorn", "--workers", "2", "--threads", "8", "--worker-class", "gthread", \
     "--bind", "0.0.0.0:8002", "--timeout", "1800", "app:app"]
