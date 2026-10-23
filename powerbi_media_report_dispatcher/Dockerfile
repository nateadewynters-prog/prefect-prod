# Media dispatcher pulls from BigQuery (not SQL Server), so unlike the sales
# dispatcher we do NOT need the Microsoft ODBC driver. Keeps the image small.
FROM python:3.11-slim-bookworm

# Container defaults to UTC with no tzdata installed. Set this so SQLite's
# datetime('now','localtime') in state.py matches Europe/London (BST/GMT),
# not UTC — otherwise every "Sent at HH:MM" on the board is an hour out
# during BST.
ENV TZ=Europe/London
RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8002

# Threads so the SSE stream and the polling requests don't block each other.
# Long timeout because a full Power BI export can take a few minutes.
CMD ["gunicorn", "--workers", "2", "--threads", "8", "--worker-class", "gthread", \
     "--bind", "0.0.0.0:8002", "--timeout", "1800", "app:app"]