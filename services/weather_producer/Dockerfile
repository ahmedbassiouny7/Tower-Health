FROM python:3.11-slim

WORKDIR /app

# Install Kafka client dependency before copying source for better Docker cache use.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# weather_producer is the service loop; weatherapi_client contains API mapping helpers.
COPY weatherapi_client.py weather_producer.py ./

CMD ["python", "weather_producer.py"]
