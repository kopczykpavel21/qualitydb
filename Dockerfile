FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY QualityDB/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt 2>/dev/null || true

# Copy application code
COPY QualityDB/ ./QualityDB/

# The database lives on a persistent Fly volume mounted at /data
# We symlink it so the app finds it at the expected path
RUN mkdir -p /data

EXPOSE 8080

CMD ["python3", "QualityDB/server.py"]
