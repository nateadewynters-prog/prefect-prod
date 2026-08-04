# Use Debian 12 (Bookworm) slim image
FROM python:3.11-slim-bookworm

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (see .dockerignore for what is excluded)
COPY . .

# Expose the internal Flask port
EXPOSE 8004

# Confirms the app is up AND its database is reachable, so a wedged container
# gets restarted by `restart: unless-stopped` instead of sitting there broken.
HEALTHCHECK --interval=60s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8004/healthz', timeout=5).status == 200 else 1)"

# ONE worker, many threads. This is deliberate.
#
# Report data used to live in a module-level variable, so each of the 4 workers
# had its own copy: the page could be rendered by one worker and the refresh
# click handled by another, which had a different list. With under 10 concurrent
# users a single worker has ample capacity and that whole class of bug is gone.
#
# Threads are generous because each in-progress refresh holds one thread open
# for the life of its SSE stream.
#
# --timeout must stay ABOVE the app's POLL_TIMEOUT_SECONDS (default 1800) and
# LOCK_TTL_SECONDS (default 2100). If gunicorn killed the worker first, the
# cleanup that releases the dataset lock would never run.
CMD ["gunicorn", "--workers", "1", "--threads", "16", "--worker-class", "gthread", \
     "--bind", "0.0.0.0:8004", "--timeout", "2400", "--access-logfile", "-", "app:app"]
