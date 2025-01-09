FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN apt-get update && apt-get install -y netcat-openbsd
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["./wait-for-postgres.sh", "python3", "main.py" ]
