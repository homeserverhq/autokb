FROM python:3.12-slim

WORKDIR /app

# Copy pyproject.toml for dependency installation
COPY pyproject.toml .

# Install dependencies
RUN pip install --no-cache-dir .

# Copy application code
COPY src/ ./src/

# Environment variables
ENV PYTHONUNBUFFERED=1

# Expose MCP port
EXPOSE 80

# Run the server
CMD ["python", "-m", "src.main"]
