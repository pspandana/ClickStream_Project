# Use Python 3.9 as base image
FROM python:3.9-slim

# Set working directory in container
WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy project files
COPY . .

# Command to run the application
CMD ["python", "data_generator.py"]
