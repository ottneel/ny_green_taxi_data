FROM python:3.9

# Set working directory
WORKDIR /app

# Update package index and install wget
RUN apt-get update && apt-get install -y wget

# Install Python dependencies
RUN pip install pandas sqlalchemy psycopg2

# Copy the Python script into the container
COPY green_taxi_pipeline.py .

# Set the entry point for the container
ENTRYPOINT [ "python", "green_taxi_pipeline.py" ]
