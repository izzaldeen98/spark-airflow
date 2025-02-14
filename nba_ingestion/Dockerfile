# Use an official Python image
FROM python:3.8-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the script and install dependencies
COPY kaggle_ingest.py /app/
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Define the entrypoint for the container
ENTRYPOINT ["python", "kaggle_ingest.py"]
