from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:  # Keeps the app importable during local smoke checks.
    st_autorefresh = None


st.set_page_config(
    page_title="Telecom Network Health Dashboard",
    page_icon="📡",
    layout="wide",
)

if st_autorefresh is not None:
    st_autorefresh(interval=30_000, key="dashboard_refresh")


@st.cache_resource
def make_engine():
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "towerhealth")
    user = os.getenv("POSTGRES_USER", "towerhealth")
    password = os.getenv("POSTGRES_PASSWORD", "towerhealth")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def table_exists(conn, table_name: str) -> bool:
    return bool(
        conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                )
                """
            ),
            {"table_name": table_name},
        ).scalar()
    )


@st.cache_data(ttl=30)
def load_table(table_name: str, order_column: str | None = "ingested_at") -> pd.DataFrame:
    engine = make_engine()
    with engine.connect() as conn:
        if not table_exists(conn, table_name):
            return pd.DataFrame()

        columns = pd.read_sql(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :table_name
                """
            ),
            conn,
            params={"table_name": table_name},
        )["column_name"].tolist()

        order_sql = f" ORDER BY {order_column} DESC" if order_column in columns else ""
        return pd.read_sql(text(f"SELECT * FROM {table_name}{order_sql}"), conn)


@st.cache_data(ttl=30)
def load_kafka_debug() -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = make_engine()
    with engine.connect() as conn:
        if not table_exists(conn, "kafka_events"):
            return pd.DataFrame(), pd.DataFrame()

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
        return counts, latest


def has_columns(df: pd.DataFrame, columns: list[str]) -> bool:
    return not df.empty and all(column in df.columns for column in columns)


def empty_notice(message: str) -> None:
    st.info(message)


def metric_value(df: pd.DataFrame, column: str, agg: str, default=0):
    if df.empty or column not in df.columns:
        return default
    series = pd.to_numeric(df[column], errors="coerce").dropna()
    if series.empty:
        return default
    if agg == "sum":
        return int(series.sum())
    if agg == "mean":
        return round(series.mean(), 2)
    return default


engine = make_engine()
ran_df = load_table("processed_ran_metrics")
weather_df = load_table("weather_metrics")

st.title("📡 Telecom Network Health Dashboard")
st.caption("Real-time RAN and weather monitoring")

overview_tab, ran_tab, weather_tab, correlation_tab, debug_tab = st.tabs(
    ["Overview", "RAN", "Weather", "Correlation", "Stream Debug"]
)

with overview_tab:
    st.subheader("Network Overview")
    col1, col2, col3, col4, col5 = st.columns(5)

    total_sites = ran_df["site_id"].nunique() if "site_id" in ran_df.columns else 0
    critical_alerts = (
        ran_df[ran_df["alert_severity"] == "CRITICAL"].shape[0]
        if has_columns(ran_df, ["alert_severity"])
        else 0
    )

    col1.metric("Sites", total_sites)
    col2.metric("Users", metric_value(ran_df, "users", "sum"))
    col3.metric("Avg SINR", metric_value(ran_df, "sinr", "mean"))
    col4.metric("Avg RSRP", metric_value(ran_df, "rsrp", "mean"))
    col5.metric("Critical Alerts", critical_alerts)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Signal Quality")
        if has_columns(ran_df, ["signal_quality"]):
            fig = px.pie(ran_df, names="signal_quality", hole=0.35)
            st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("No processed RAN signal quality rows yet.")

    with col2:
        st.subheader("Alert Severity")
        if has_columns(ran_df, ["alert_severity"]):
            fig = px.histogram(ran_df, x="alert_severity", color="alert_severity")
            st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("No processed RAN alert rows yet.")

with ran_tab:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top Sites By Users")
        if has_columns(ran_df, ["site_name", "users"]):
            users_df = (
                ran_df.groupby("site_name", dropna=False)["users"]
                .sum()
                .reset_index()
                .sort_values("users", ascending=False)
                .head(10)
            )
            fig = px.bar(users_df, x="site_name", y="users")
            st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("Waiting for RAN user metrics.")

    with col2:
        st.subheader("Average SINR By Site")
        if has_columns(ran_df, ["site_name", "sinr"]):
            sinr_df = ran_df.groupby("site_name", dropna=False)["sinr"].mean().reset_index()
            fig = px.bar(sinr_df, x="site_name", y="sinr")
            st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("Waiting for RAN SINR metrics.")

    st.subheader("Latest RAN Records")
    if not ran_df.empty:
        st.dataframe(ran_df.head(50), use_container_width=True, hide_index=True)
    else:
        empty_notice("No rows in processed_ran_metrics yet.")

with weather_tab:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Temperature By Region")
        if has_columns(weather_df, ["ran_region", "weather_temperature_c"]):
            region_temp = (
                weather_df.groupby("ran_region", dropna=False)["weather_temperature_c"]
                .mean()
                .reset_index()
            )
            fig = px.bar(region_temp, x="ran_region", y="weather_temperature_c")
            st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("Waiting for weather temperature metrics.")

    with col2:
        st.subheader("Humidity By Region")
        if has_columns(weather_df, ["ran_region", "weather_humidity_pct"]):
            region_humidity = (
                weather_df.groupby("ran_region", dropna=False)["weather_humidity_pct"]
                .mean()
                .reset_index()
            )
            fig = px.bar(region_humidity, x="ran_region", y="weather_humidity_pct")
            st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("Waiting for weather humidity metrics.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Weather Conditions")
        if has_columns(weather_df, ["weather_condition"]):
            fig = px.pie(weather_df, names="weather_condition", hole=0.35)
            st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("Waiting for weather condition rows.")

    with col2:
        st.subheader("Rain Intensity")
        if has_columns(weather_df, ["rain_intensity"]):
            fig = px.pie(weather_df, names="rain_intensity", hole=0.35)
            st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("Waiting for rain metrics.")

    st.subheader("Latest Weather Records")
    if not weather_df.empty:
        st.dataframe(weather_df.head(50), use_container_width=True, hide_index=True)
    else:
        empty_notice("No rows in weather_metrics yet.")

with correlation_tab:
    st.subheader("Telecom vs Weather Correlation")
    if has_columns(ran_df, ["site_id", "sinr", "rsrp"]) and has_columns(
        weather_df,
        ["ran_site_id", "weather_temperature_c", "weather_wind_speed_kmh"],
    ):
        merged_df = pd.merge(
            ran_df,
            weather_df,
            left_on="site_id",
            right_on="ran_site_id",
            how="inner",
            suffixes=("_ran", "_weather"),
        )
        if not merged_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.scatter(
                    merged_df,
                    x="weather_temperature_c",
                    y="sinr",
                    color="site_id",
                    title="SINR vs Temperature",
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.scatter(
                    merged_df,
                    x="weather_wind_speed_kmh",
                    y="rsrp",
                    color="site_id",
                    title="RSRP vs Wind Speed",
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            empty_notice("RAN and weather rows exist, but site IDs do not overlap yet.")
    else:
        empty_notice("Correlation charts will appear after both processed streams write rows.")

with debug_tab:
    counts, latest = load_kafka_debug()
    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("Kafka Event Counts")
        if not counts.empty:
            st.dataframe(counts, use_container_width=True, hide_index=True)
        else:
            empty_notice("No raw Kafka landing rows yet.")
    with col2:
        st.subheader("Latest Raw Events")
        if not latest.empty:
            st.dataframe(latest, use_container_width=True, hide_index=True)
        else:
            empty_notice("No latest raw events to show.")
