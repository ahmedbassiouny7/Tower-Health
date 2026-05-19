from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


def make_engine():
    """Create a SQLAlchemy connection using the same env vars as Compose."""
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "towerhealth")
    user = os.getenv("POSTGRES_USER", "towerhealth")
    password = os.getenv("POSTGRES_PASSWORD", "towerhealth")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


st.set_page_config(page_title="TowerHealth Realtime", layout="wide")
st.title("TowerHealth Realtime")

engine = make_engine()

with engine.connect() as conn:
    # Summary table: one row per Kafka topic, useful for checking ingestion.
    counts = pd.read_sql(
        text(
            """
            SELECT topic, COUNT(*) AS messages, MAX(event_time) AS latest_event
            FROM kafka_events
            GROUP BY topic
            ORDER BY topic
            """
        ),
        conn,
    )

    # Raw latest events table: keeps offsets and message JSON visible for debug.
    latest = pd.read_sql(
        text(
            """
            SELECT topic, kafka_partition, kafka_offset, event_time, message_key, message_value
            FROM kafka_events
            ORDER BY event_time DESC
            LIMIT 50
            """
        ),
        conn,
    )

left, right = st.columns([1, 2])

with left:
    st.subheader("Message Counts")
    st.dataframe(counts, use_container_width=True, hide_index=True)

with right:
    st.subheader("Latest Events")
    st.dataframe(latest, use_container_width=True, hide_index=True)
