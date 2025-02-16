# Use the official Python image.
FROM python:3.9-slim

# Set environment variables.
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory.
WORKDIR /app

# Copy the requirements file and install dependencies.
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of the application code.
COPY . .

# Expose port 8080.
EXPOSE 8080

# Start the application using Gunicorn.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]