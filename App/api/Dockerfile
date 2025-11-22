# --------------------------------------------------------------
# Base image
FROM python:3.11-slim

# --------------------------------------------------------------
# Working directory (the API lives under App/api)
WORKDIR /app/App/api

# --------------------------------------------------------------
# System dependencies
#   - build-essential, wget, tar : needed to compile TA‑Lib
#   - git                         : required for pip‑installing git+ URLs
#   - curl                        : used by the health‑check
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    tar \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# --------------------------------------------------------------
# Compile TA‑Lib from source (the official C library)
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xvzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# --------------------------------------------------------------
# Install Python dependencies
COPY App/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------------------------------
# Copy the whole project (so the API can import shared code)
COPY . /app

# --------------------------------------------------------------
# Expose the FastAPI port
EXPOSE 8000

# --------------------------------------------------------------
# Health‑check (makes the container report “healthy” to Docker)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# --------------------------------------------------------------
# Start the server (the “user‑first” command you wanted)
CMD ["python", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--access-log"]