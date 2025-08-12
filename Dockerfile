# Use official Python image with version 3.10+
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install pipenv for dependency management
RUN pip install pipenv

# Copy Pipfile and Pipfile.lock if present
COPY Pipfile* ./

# Install dependencies
RUN pipenv install --deploy --system --dev

# Copy the rest of your application code
COPY . .

# Expose any ports if needed (optional, adjust as necessary)
# EXPOSE 8000

# Default command to run the service (adjust --mode as needed)
CMD ["python", "-m", "sinitaivas_live.main", "--mode", "fresh"]
