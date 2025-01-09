# Use the official Python image as a base
FROM python:3.10-slim

# Set the working directory inside the container to the `api` folder
WORKDIR /app/api

# Copy the `requirements.txt` file into the container
COPY requirements.txt /app/api/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire `api` directory into the container
COPY . /app/api/

# Expose the application port
EXPOSE 5050

# Run the FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5050"]
