# TowerHealth Local Stack

This is the current local development shape:

```text
RAN generator + Weather producer
        -> Kafka broker
        -> Kafka Connect S3 sink every 15 minutes
        -> Spark Structured Streaming in real time
        -> Postgres
        -> Streamlit dashboard
```

## Current Kafka Mode

For now, the project uses one Kafka broker:

```text
broker:29092
```

The final edit can replace this with a 3-broker cluster.

## Topics

```text
ran_telemetry
weather_events
```

## UIs

Kafka UI:

```text
http://localhost:8090
```

Spark master UI:

```text
http://localhost:8084
```

Streamlit realtime dashboard:

```text
http://localhost:8501
```

## S3 Connector

Kafka Connect runs on:

```text
http://localhost:8083
```

The `s3-connector-init` service creates connector:

```text
s3-bronze-sink
```

It reads:

```text
ran_telemetry,weather_events
```

It writes to S3 every 15 minutes using:

```env
S3_ROTATE_INTERVAL_MS=900000
```

Fill these in before expecting S3 writes to succeed:

```env
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
AWS_REGION=
S3_BUCKET_NAME=
```

The IAM user or role used by Kafka Connect must be allowed to write objects
under the connector prefix. For the local `.env` default:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::tower-iti-project"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::tower-iti-project/raw-data/*"
    }
  ]
}
```

## Run

```powershell
docker compose up -d
```

Check status:

```powershell
docker compose ps
```

Watch logs:

```powershell
docker compose logs -f ran-generator weather-producer spark-stream connect s3-connector-init
```
