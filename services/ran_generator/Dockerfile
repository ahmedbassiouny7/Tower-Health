FROM python:3.11-slim

WORKDIR /app

# Install producer dependencies first so Docker can reuse this layer when only
# the generator code changes.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the simulator entrypoint that emits RAN telemetry to stdout, files, or Kafka.
COPY ran_data_generator.py ./

CMD ["python", "ran_data_generator.py"]
