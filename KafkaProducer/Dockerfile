# Dockerfile for Kafka Producer

# Use an official Python base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer code into the container
COPY . /app

# Expose the port for Flask
EXPOSE 5010

# Command to run the producer
CMD ["python", "producer.py"]
