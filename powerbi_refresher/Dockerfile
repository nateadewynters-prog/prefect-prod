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
# The worker timeout must stay ABOVE the app's LOCK_TTL_SECONDS, which in turn
# stays above POLL_TIMEOUT_SECONDS. If gunicorn killed the worker first, the
# cleanup that releases the dataset lock would never run.
#
# It is set here as an environment variable and referenced in the command below
# so there is ONE source of truth: app.py reads the same variable and refuses to
# start if the ordering is wrong. Hard-coding it in both places let them drift.
ENV GUNICORN_TIMEOUT=2400

# Shell form so $GUNICORN_TIMEOUT is expanded; `exec` keeps gunicorn as PID 1 so
# it still receives SIGTERM directly from `docker stop`.
CMD exec gunicorn --workers 1 --threads 16 --worker-class gthread \
     --bind 0.0.0.0:8004 --timeout $GUNICORN_TIMEOUT --access-logfile - app:app
