FROM python:3.12-slim

# Create a non-root user
RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

# Install dependencies first for better layer caching.
# cryptg and pillow ship manylinux wheels, so no compiler is needed.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Create runtime directories and fix ownership
RUN mkdir -p logs .sessions \
    && chown -R appuser:appuser /app

USER appuser

CMD ["python", "main.py"]
