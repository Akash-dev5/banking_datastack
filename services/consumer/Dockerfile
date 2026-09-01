FROM python:3.12-slim

WORKDIR /app

COPY services/consumer/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/consumer/kafka_to_s3.py .

CMD ["python", "kafka_to_s3.py"]