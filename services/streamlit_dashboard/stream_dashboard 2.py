import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# ==========================================
# PAGE CONFIG
# ==========================================

st.set_page_config(
    page_title="Telecom Network Health Dashboard",
    page_icon="📡",
    layout="wide"
)

st_autorefresh(interval=30000, key="refresh")

st.title("📡 Telecom Network Health Dashboard")
st.markdown("Real-Time RAN & Weather Monitoring")

# ==========================================
# DATABASE CONNECTION
# ==========================================

@st.cache_resource
def get_engine():
    return create_engine(
        "postgresql://towerhealth:towerhealth@localhost:5432/towerhealth"
    )

engine = get_engine()

# ==========================================
# LOAD DATA
# ==========================================

@st.cache_data(ttl=30)
def load_data():
    try:
        ran_df = pd.read_sql(
            "SELECT * FROM processed_ran_metrics",
            engine
        )
    except Exception:
        ran_df = pd.DataFrame()

    try:
        weather_df = pd.read_sql(
            "SELECT * FROM weather_metrics",
            engine
        )
    except Exception:
        weather_df = pd.DataFrame()

    return ran_df, weather_df

ran_df, weather_df = load_data()

# ==========================================
# KPI SECTION
# ==========================================

st.subheader("📊 Network Overview")

col1, col2, col3, col4, col5 = st.columns(5)

total_sites = ran_df["site_id"].nunique() if not ran_df.empty else 0
total_users = int(ran_df["users"].sum()) if not ran_df.empty else 0
avg_sinr = round(ran_df["sinr"].mean(), 2) if not ran_df.empty else 0
avg_rsrp = round(ran_df["rsrp"].mean(), 2) if not ran_df.empty else 0
critical_alerts = (
    ran_df[ran_df["alert_severity"] == "CRITICAL"].shape[0]
    if not ran_df.empty else 0
)

col1.metric("Sites", total_sites)
col2.metric("Users", total_users)
col3.metric("Avg SINR", avg_sinr)
col4.metric("Avg RSRP", avg_rsrp)
col5.metric("Critical Alerts", critical_alerts)

st.divider()

# ==========================================
# ROW 1
# ==========================================

col1, col2 = st.columns(2)

with col1:
    st.subheader("Signal Quality Distribution")
    if not ran_df.empty and "signal_quality" in ran_df.columns:
        fig = px.pie(ran_df, names="signal_quality")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Alert Severity Distribution")
    if not ran_df.empty and "alert_severity" in ran_df.columns:
        fig = px.histogram(ran_df, x="alert_severity")
        st.plotly_chart(fig, use_container_width=True)

# ==========================================
# ROW 2
# ==========================================

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Sites By Users")
    if not ran_df.empty:
        users_df = (
            ran_df.groupby("site_name")["users"]
            .sum()
            .reset_index()
            .sort_values("users", ascending=False)
            .head(10)
        )
        fig = px.bar(users_df, x="site_name", y="users")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Average SINR By Site")
    if not ran_df.empty:
        sinr_df = (
            ran_df.groupby("site_name")["sinr"]
            .mean()
            .reset_index()
        )
        fig = px.bar(sinr_df, x="site_name", y="sinr")
        st.plotly_chart(fig, use_container_width=True)

# ==========================================
# ROW 3
# ==========================================

col1, col2 = st.columns(2)

with col1:
    st.subheader("Temperature By Region")
    if not weather_df.empty and "ran_region" in weather_df.columns:
        region_temp = (
            weather_df.groupby("ran_region")["weather_temperature_c"]
            .mean()
            .reset_index()
        )
        fig = px.bar(region_temp, x="ran_region", y="weather_temperature_c")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Humidity By Region")
    if not weather_df.empty and "ran_region" in weather_df.columns:
        region_humidity = (
            weather_df.groupby("ran_region")["weather_humidity_pct"]
            .mean()
            .reset_index()
        )
        fig = px.bar(region_humidity, x="ran_region", y="weather_humidity_pct")
        st.plotly_chart(fig, use_container_width=True)

# ==========================================
# ROW 4
# ==========================================

col1, col2 = st.columns(2)

with col1:
    st.subheader("Weather Condition Distribution")
    if not weather_df.empty and "weather_condition" in weather_df.columns:
        fig = px.pie(weather_df, names="weather_condition")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Rain Intensity Distribution")
    if not weather_df.empty and "rain_intensity" in weather_df.columns:
        fig = px.pie(weather_df, names="rain_intensity")
        st.plotly_chart(fig, use_container_width=True)

# ==========================================
# CORRELATION SECTION
# ==========================================

st.divider()
st.subheader("📈 Telecom vs Weather Correlation")

if not ran_df.empty and not weather_df.empty:
    merged_df = pd.merge(
        ran_df,
        weather_df,
        left_on="site_id",
        right_on="ran_site_id",
        how="inner"
    )

    if not merged_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.scatter(
                merged_df,
                x="weather_temperature_c",
                y="sinr",
                title="SINR vs Temperature"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.scatter(
                merged_df,
                x="weather_wind_speed_kmh",
                y="rsrp",
                title="RSRP vs Wind Speed"
            )
            st.plotly_chart(fig, use_container_width=True)

# ==========================================
# LIVE DATA TABLES
# ==========================================

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Latest RAN Records")
    if not ran_df.empty:
        st.dataframe(ran_df.tail(10), use_container_width=True)

with col2:
    st.subheader("Latest Weather Records")
    if not weather_df.empty:
        st.dataframe(weather_df.tail(10), use_container_width=True)
