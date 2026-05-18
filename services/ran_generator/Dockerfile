FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ran_data_generator.py ./

CMD ["python", "ran_data_generator.py"]
