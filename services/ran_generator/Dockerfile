FROM python:3.11-slim

WORKDIR /app

# Install producer dependencies first so Docker can reuse this layer when only
# the generator code changes.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the simulator entrypoints.
COPY ran_data_generator.py ./
COPY historical_backfill.py ./

CMD ["python", "ran_data_generator.py"]
