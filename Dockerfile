FROM python:3.10-slim

WORKDIR /app

# Install sqlite3 in case a manual shell is ever needed inside the container
RUN apt-get update && \
    apt-get install -y sqlite3 && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the actual bot code, the database, and the calibration model
COPY . .

# Make sure imports resolve correctly
ENV PYTHONPATH=/app

# Start the bot
CMD ["python", "bot.py"]
