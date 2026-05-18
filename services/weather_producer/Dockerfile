FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY weatherapi_client.py weather_producer.py ./

CMD ["python", "weather_producer.py"]
