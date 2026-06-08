from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

# ===============================
# CONFIGURATION
# ===============================
st.set_page_config(
    page_title="Tower Health NOC",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st_autorefresh(interval=10000, key="refresh")

PLOTLY_TEMPLATE = "plotly_dark"
PLOTLY_LAYOUT = dict(
    paper_bgcolor="#111827",
    plot_bgcolor="#111827",
    font=dict(color="#e5e7eb"),
)
SEVERITY_ORDER = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}

st.markdown(
    """
<style>
body { background-color: #030712; color: #f9fafb; }
[data-testid="stMetric"] {
    background-color: #111827;
    border: 1px solid #1f2937;
    padding: 14px;
    border-radius: 12px;
}
div[data-testid="stPlotlyChart"] {
    background-color: #111827;
    padding: 10px;
    border-radius: 12px;
    border: 1px solid #1f2937;
}
.section-title {
    font-size: 1.25rem;
    font-weight: 700;
    margin: 1rem 0 0.5rem 0;
    color: #93c5fd;
}
.alert-card-critical {
    background: #450a0a;
    border-left: 4px solid #ef4444;
    padding: 12px 16px;
    border-radius: 8px;
    margin-bottom: 8px;
}
.alert-card-warning {
    background: #451a03;
    border-left: 4px solid #f59e0b;
    padding: 12px 16px;
    border-radius: 8px;
    margin-bottom: 8px;
}
</style>
""",
    unsafe_allow_html=True,
)


# ===============================
# DATA LAYER
# ===============================
def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "towerhealth")
    user = os.getenv("POSTGRES_USER", "towerhealth")
    password = os.getenv("POSTGRES_PASSWORD", "towerhealth")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


engine = get_engine()


def _read_table(table_name: str) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(f"SELECT * FROM {table_name}", conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        _read_table("processed_ran_metrics"),
        _read_table("transport_metrics"),
        _read_table("weather_metrics"),
    )


def latest_snapshot(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "ingested_at" in out.columns:
        out["ingested_at"] = pd.to_datetime(out["ingested_at"], errors="coerce")
        out = out.sort_values("ingested_at")
    return out.groupby(keys, as_index=False).last()


def geo_key(lat: pd.Series, lon: pd.Series) -> pd.Series:
    return lat.round(3).astype(str) + "_" + lon.round(3).astype(str)


def query_geo_key(query: pd.Series) -> pd.Series:
    parts = query.astype(str).str.split(",", expand=True)
    lat = pd.to_numeric(parts[0], errors="coerce")
    lon = pd.to_numeric(parts[1], errors="coerce")
    return geo_key(lat, lon)


def _has_ran_site_ids(df: pd.DataFrame) -> bool:
    if "ran_site_id" not in df.columns:
        return False
    return df["ran_site_id"].fillna("").astype(str).str.strip().ne("").any()


def weather_snapshot_keys(df: pd.DataFrame) -> list[str]:
    return ["ran_site_id"] if _has_ran_site_ids(df) else ["location_query"]


def weather_geo_key(wx: pd.DataFrame) -> pd.Series:
    if "location_query" in wx.columns and wx["location_query"].fillna("").astype(str).str.strip().ne("").any():
        return query_geo_key(wx["location_query"])
    return geo_key(wx["weather_latitude"], wx["weather_longitude"])


def join_ran_weather(ran: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    if ran.empty or weather.empty:
        return pd.DataFrame()

    wx = latest_snapshot(weather, weather_snapshot_keys(weather)).copy()
    wx["_geo_key"] = weather_geo_key(wx)
    out = ran.copy()
    out["_geo_key"] = geo_key(out["lat"], out["lon"])

    merged = out.merge(wx, left_on="site_id", right_on="ran_site_id", how="left", suffixes=("", "_wx"))
    missing = merged["weather_temperature_c"].isna()
    if missing.any():
        geo_part = out.loc[missing, ["site_id", "cell_id", "_geo_key"]].merge(
            wx.drop(columns=["ran_site_id"], errors="ignore"), on="_geo_key", how="left"
        )
        for col in [
            "ran_site_id", "ran_site_name", "weather_temperature_c", "weather_humidity_pct",
            "weather_rainfall_mm", "weather_wind_speed_kmh", "weather_condition",
            "is_raining", "rain_intensity", "weather_location_name",
        ]:
            if col in geo_part.columns:
                merged.loc[missing, col] = geo_part[col].values
    return merged.drop(columns=["_geo_key", "_geo_key_wx"], errors="ignore")


def site_level_insights(merged: pd.DataFrame) -> pd.DataFrame:
    if merged.empty:
        return merged
    return merged.groupby(["site_id", "site_name"], as_index=False).agg(
        users=("users", "sum"),
        cells=("cell_id", "count"),
        avg_sinr=("sinr", "mean"),
        avg_rsrp=("rsrp", "mean"),
        avg_cqi=("cqi", "mean"),
        avg_downlink_mbps=("downlink_mbps", "mean"),
        poor_cells=("signal_quality", lambda s: int((s == "Poor").sum())),
        weather_temperature_c=("weather_temperature_c", "first"),
        weather_humidity_pct=("weather_humidity_pct", "first"),
        weather_rainfall_mm=("weather_rainfall_mm", "first"),
        weather_wind_speed_kmh=("weather_wind_speed_kmh", "first"),
        weather_condition=("weather_condition", "first"),
        is_raining=("is_raining", "first"),
        rain_intensity=("rain_intensity", "first"),
        battery_status=("battery_status", "first"),
        battery_charge=("battery_charge", "first"),
        alert_severity=("alert_severity", "first"),
    )


def apply_plotly_style(fig: go.Figure) -> go.Figure:
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


def filter_last_hour(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "ingested_at" not in df.columns:
        return df
    out = df.copy()
    out["ingested_at"] = pd.to_datetime(out["ingested_at"], errors="coerce")
    cutoff = out["ingested_at"].max() - timedelta(hours=1)
    if pd.isna(cutoff):
        return out
    return out[out["ingested_at"] >= cutoff]


def build_alert_feed(
    ran_latest: pd.DataFrame,
    transport_latest: pd.DataFrame,
    ran_weather: pd.DataFrame,
) -> list[dict]:
    alerts: list[dict] = []
    seen: set[str] = set()

    def add(severity: str, category: str, site: str, message: str, ts=None):
        key = f"{severity}|{site}|{message}"
        if key in seen:
            return
        seen.add(key)
        alerts.append({
            "severity": severity,
            "category": category,
            "site": site,
            "message": message,
            "time": ts,
        })

    for _, row in ran_latest.iterrows():
        site = row.get("site_name", "Unknown")
        ts = row.get("ingested_at")
        cell = row.get("cell_id", "?")

        if row.get("alert_severity") == "CRITICAL":
            add("CRITICAL", "Network", site,
                f"Critical alert at {site} — highest severity on cell {cell}", ts)

        bat_status = str(row.get("battery_status", "")).upper()
        if bat_status in ("DOWN", "FAILED"):
            add("CRITICAL", "Power", site,
                f"Damaged battery at {site} — battery status: {row.get('battery_status')}", ts)
        elif pd.notna(row.get("battery_charge")) and row["battery_charge"] < 20:
            add("WARNING", "Power", site,
                f"Low battery at {site} — charge {row['battery_charge']:.0f}%", ts)

        if row.get("cell_status") != "UP":
            add("CRITICAL", "Radio", site,
                f"Cell {cell} is DOWN at {site} — check RU/antenna/backhaul", ts)
        elif row.get("signal_quality") == "Poor":
            add("WARNING", "Signal", site,
                f"Poor signal on {cell} at {site} — RSRP {row.get('rsrp', 0):.1f} dBm", ts)

        if pd.notna(row.get("ho_success_rate")) and row["ho_success_rate"] < 93:
            add("WARNING", "Mobility", site,
                f"Low handover success on {cell} at {site} — HSR {row['ho_success_rate']:.1f}%", ts)

    for _, row in transport_latest.iterrows():
        site = row.get("site_name", "Unknown")
        ts = row.get("ingested_at")
        link = row.get("link_id", "?")
        if row.get("severity") == "CRITICAL":
            add("CRITICAL", "Transport", site,
                f"Backhaul CRITICAL on link {link} at {site} — "
                f"latency {row.get('latency_ms', 0):.1f}ms, loss {row.get('packet_loss_percent', 0):.2f}%", ts)
        elif row.get("severity") == "WARNING":
            add("WARNING", "Transport", site,
                f"Backhaul degradation on {link} at {site} — utilization {row.get('utilization_percent', 0):.1f}%", ts)

    if not ran_weather.empty:
        matched = ran_weather.dropna(subset=["weather_temperature_c"])
        for _, row in matched.iterrows():
            site = row.get("site_name", "Unknown")
            ts = row.get("ingested_at")
            if row.get("rain_intensity") in ("Moderate", "Heavy") and row.get("signal_quality") == "Poor":
                add("WARNING", "Weather", site,
                    f"Heavy rainfall ({row.get('rain_intensity')}) may be affecting {row.get('cell_id')} "
                    f"at {site} — SINR {row.get('sinr', 0):.1f} dB", ts)
            if row.get("is_raining") and pd.notna(row.get("downlink_mbps")) and row["downlink_mbps"] < 100:
                add("WARNING", "Weather", site,
                    f"Throughput drop during rain at {site} — downlink {row['downlink_mbps']:.0f} Mbps", ts)

    alerts.sort(key=lambda a: (SEVERITY_ORDER.get(a["severity"], 9), str(a.get("time") or "")))
    return alerts


def render_alert_feed(alerts: list[dict], max_items: int = 8):
    if not alerts:
        st.success("✅ No active alerts — network operating normally.")
        return

    critical = [a for a in alerts if a["severity"] == "CRITICAL"]
    warning = [a for a in alerts if a["severity"] == "WARNING"]
    st.markdown(f"**{len(critical)} critical** · **{len(warning)} warnings** · {len(alerts)} total")

    for alert in alerts[:max_items]:
        css = "alert-card-critical" if alert["severity"] == "CRITICAL" else "alert-card-warning"
        icon = "🚨" if alert["severity"] == "CRITICAL" else "⚠️"
        st.markdown(
            f'<div class="{css}">{icon} <b>[{alert["category"]}]</b> {alert["site"]}: {alert["message"]}</div>',
            unsafe_allow_html=True,
        )
    if len(alerts) > max_items:
        st.caption(f"+ {len(alerts) - max_items} more alerts")


def render_header():
    c1, c2 = st.columns([8, 2])
    c1.title("📡 Tower Health — NOC")
    c2.success("🟢 LIVE")
    st.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · auto-refresh 10s")


def infer_cell_cause(row: pd.Series) -> str:
    if row.get("cell_status") != "UP":
        return "Cell radio unit or sector reported DOWN — possible hardware or power fault."
    if row.get("signal_quality") == "Poor":
        return f"Weak RF coverage — RSRP {row.get('rsrp', 0):.1f} dBm, possible interference or weather fade."
    if pd.notna(row.get("ho_success_rate")) and row["ho_success_rate"] < 93:
        return f"Mobility issues — handover success {row['ho_success_rate']:.1f}% below threshold."
    if str(row.get("battery_status", "")).upper() in ("DOWN", "FAILED"):
        return "Site power/battery fault may be impacting cell availability."
    return "No major fault detected — cell operating within normal parameters."


# ===============================
# LOAD DATA
# ===============================
ran_df, transport_df, weather_df = load_data()
ran_latest = latest_snapshot(ran_df, ["site_id", "cell_id"])
transport_latest = latest_snapshot(transport_df, ["site_id", "link_id"])
weather_latest = latest_snapshot(weather_df, weather_snapshot_keys(weather_df))
ran_weather = join_ran_weather(ran_latest, weather_df)
site_insights = site_level_insights(ran_weather)
alert_feed = build_alert_feed(ran_latest, transport_latest, ran_weather)
ran_hour = filter_last_hour(ran_df)
transport_hour = filter_last_hour(transport_df)


# ===============================
# PAGE 1 — NETWORK OVERVIEW
# ===============================
def page_network_overview():
    render_header()
    st.markdown('<p class="section-title">🚨 Active Alerts</p>', unsafe_allow_html=True)
    render_alert_feed(alert_feed)

    if ran_latest.empty:
        st.warning("Waiting for Kafka → Spark → Postgres pipeline.")
        return

    active_cells = int((ran_latest["cell_status"] == "UP").sum())
    total_cells = len(ran_latest)
    total_users = int(ran_latest["users"].sum())
    critical_count = sum(1 for a in alert_feed if a["severity"] == "CRITICAL")

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("👥 Active Users", f"{total_users:,}")
    k2.metric("✅ Cells UP", f"{active_cells}/{total_cells}")
    k3.metric("🚨 Critical Alerts", critical_count)
    k4.metric("⚡ Avg SINR", f"{ran_latest['sinr'].mean():.1f} dB")
    k5.metric("🔋 Avg Battery", f"{ran_latest['battery_charge'].mean():.0f}%")

    st.markdown('<p class="section-title">📈 Performance — Last Hour</p>', unsafe_allow_html=True)
    t1, t2 = st.columns(2)

    with t1:
        if not ran_hour.empty and "ingested_at" in ran_hour.columns:
            tp = (
                ran_hour.groupby("ingested_at", as_index=False)
                .agg(throughput_mbps=("downlink_mbps", "mean"))
                .sort_values("ingested_at")
            )
            fig = px.line(
                tp, x="ingested_at", y="throughput_mbps",
                markers=True, template=PLOTLY_TEMPLATE,
                labels={"ingested_at": "Time", "throughput_mbps": "Avg Downlink (Mbps)"},
                title="Network Throughput (hourly trend)",
            )
            st.plotly_chart(apply_plotly_style(fig), use_container_width=True)
        else:
            st.info("Collecting throughput history — check back after a few streaming batches.")

    with t2:
        if not transport_hour.empty and "ingested_at" in transport_hour.columns:
            lat = (
                transport_hour.groupby("ingested_at", as_index=False)
                .agg(latency_ms=("latency_ms", "mean"))
                .sort_values("ingested_at")
            )
            fig = px.line(
                lat, x="ingested_at", y="latency_ms",
                markers=True, template=PLOTLY_TEMPLATE,
                labels={"ingested_at": "Time", "latency_ms": "Avg Latency (ms)"},
                title="Backhaul Latency (hourly trend)",
            )
            st.plotly_chart(apply_plotly_style(fig), use_container_width=True)
        else:
            st.info("No transport latency history yet.")

    st.markdown('<p class="section-title">🗺️ Network Map</p>', unsafe_allow_html=True)
    site_map = (
        ran_latest.groupby(["site_id", "site_name", "lat", "lon"], as_index=False)
        .agg(downlink_mbps=("downlink_mbps", "mean"), users=("users", "sum"),
             alert_severity=("alert_severity", "first"))
    )
    if not site_map.empty and site_map["lat"].notna().any():
        fig = px.scatter_geo(
            site_map, lat="lat", lon="lon", color="downlink_mbps", size="users",
            hover_name="site_name", color_continuous_scale="Turbo", template=PLOTLY_TEMPLATE,
        )
        fig.update_geos(fitbounds="locations", showcountries=True, landcolor="#1f2937", oceancolor="#030712")
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)


# ===============================
# PAGE 2 — TOWER INSIGHTS
# ===============================
def page_tower_insights():
    render_header()
    if ran_latest.empty:
        st.warning("No tower data available.")
        return

    sites = sorted(ran_latest["site_name"].unique())
    selected = st.selectbox("Select Tower", sites, key="tower_select")

    tower_cells = ran_latest[ran_latest["site_name"] == selected]
    tower_merged = ran_weather[ran_weather["site_name"] == selected] if not ran_weather.empty else tower_cells
    tower_alerts = [a for a in alert_feed if a["site"] == selected]

    st.markdown(f'<p class="section-title">🗼 {selected}</p>', unsafe_allow_html=True)

    if tower_alerts:
        st.markdown("**Alerts for this tower**")
        render_alert_feed(tower_alerts, max_items=6)
    else:
        st.success(f"✅ {selected} — no active alerts")

    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Cells", len(tower_cells))
    t2.metric("Users", int(tower_cells["users"].sum()))
    t3.metric("Avg SINR", f"{tower_cells['sinr'].mean():.1f} dB")
    t4.metric("Avg HSR", f"{tower_cells['ho_success_rate'].mean():.1f}%")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Signal Quality")
        sq = tower_cells["signal_quality"].value_counts().reset_index()
        sq.columns = ["quality", "count"]
        fig = px.pie(sq, names="quality", values="count",
                     color="quality", color_discrete_map={"Excellent": "#22c55e", "Good": "#3b82f6", "Poor": "#ef4444"},
                     template=PLOTLY_TEMPLATE)
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)

    with c2:
        st.subheader("RSRP vs RSRQ")
        fig = px.scatter(tower_cells, x="rsrp", y="rsrq", color="cell_id", trendline="ols",
                         template=PLOTLY_TEMPLATE, labels={"rsrp": "RSRP (dBm)", "rsrq": "RSRQ (dB)"})
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Handover Success by Cell")
        fig = px.bar(tower_cells, x="cell_id", y="ho_success_rate", color="ho_success_rate",
                     color_continuous_scale="RdYlGn", range_color=[90, 100], template=PLOTLY_TEMPLATE)
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)

    with c4:
        st.subheader("Tower Health Summary")
        if not site_insights.empty:
            row = site_insights[site_insights["site_name"] == selected]
            if not row.empty:
                r = row.iloc[0]
                st.write(f"**Vendor region cells:** {int(r['cells'])}")
                st.write(f"**Poor signal cells:** {int(r['poor_cells'])}")
                st.write(f"**Battery:** {r.get('battery_status', 'N/A')} ({r.get('battery_charge', 0):.0f}%)")
                if r.get("is_raining"):
                    st.write(f"**Weather:** 🌧️ {r.get('weather_condition')} — {r.get('rain_intensity')} rain")
                else:
                    st.write(f"**Weather:** {r.get('weather_condition', 'N/A')}")

    tower_transport = transport_latest[transport_latest["site_name"] == selected]
    if not tower_transport.empty:
        st.subheader("Backhaul Links")
        st.dataframe(
            tower_transport[["link_id", "link_type", "link_status", "latency_ms",
                             "utilization_percent", "severity"]],
            use_container_width=True, hide_index=True,
        )


# ===============================
# PAGE 3 — CELL DIAGNOSTICS
# ===============================
def page_cell_diagnostics():
    render_header()
    if ran_latest.empty:
        st.warning("No cell data available.")
        return

    labels = [f"{r['site_name']} / {r['cell_id']} ({r['tech']})" for _, r in ran_latest.iterrows()]
    idx = st.selectbox("Select Cell", range(len(labels)), format_func=lambda i: labels[i], key="cell_select")
    cell = ran_latest.iloc[idx]
    cell_wx = (
        ran_weather[(ran_weather["site_id"] == cell["site_id"]) & (ran_weather["cell_id"] == cell["cell_id"])]
        if not ran_weather.empty else pd.DataFrame()
    )

    status_color = "🟢" if cell["cell_status"] == "UP" else "🔴"
    st.markdown(f'<p class="section-title">{status_color} {cell["cell_id"]} @ {cell["site_name"]}</p>',
                unsafe_allow_html=True)

    cell_alerts = [a for a in alert_feed if cell["cell_id"] in a["message"] and cell["site_name"] == a["site"]]
    if cell_alerts:
        render_alert_feed(cell_alerts, max_items=5)
    else:
        st.success("No alerts for this cell.")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Status", cell["cell_status"])
    m2.metric("Signal", cell["signal_quality"])
    m3.metric("Users", int(cell["users"]))
    m4.metric("RSRP", f"{cell['rsrp']:.1f} dBm")
    m5.metric("Downlink", f"{cell['downlink_mbps']:.0f} Mbps")

    st.subheader("📍 Location")
    loc1, loc2 = st.columns([1, 2])
    with loc1:
        st.write(f"**Site ID:** {cell['site_id']}")
        st.write(f"**Latitude:** {cell['lat']:.4f}")
        st.write(f"**Longitude:** {cell['lon']:.4f}")
        st.write(f"**Technology:** {cell['tech']}")
        if not cell_wx.empty and pd.notna(cell_wx.iloc[0].get("weather_condition")):
            wx = cell_wx.iloc[0]
            st.write(f"**Weather:** {wx.get('weather_condition')} · {wx.get('weather_temperature_c', 0):.0f}°C")
            if wx.get("is_raining"):
                st.write(f"**Rain:** {wx.get('rain_intensity')} ({wx.get('weather_rainfall_mm', 0):.1f} mm)")
    with loc2:
        fig = px.scatter_geo(
            pd.DataFrame([cell]), lat="lat", lon="lon", text="cell_id",
            template=PLOTLY_TEMPLATE, title="Cell geographic position",
        )
        fig.update_geos(fitbounds="locations", showcountries=True, landcolor="#1f2937")
        fig.update_traces(marker=dict(size=14, color="#3b82f6"))
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)

    st.subheader("🔍 Diagnosis")
    cause = infer_cell_cause(cell)
    if cell["cell_status"] != "UP":
        st.error(f"**Root cause:** {cause}")
    elif cell["signal_quality"] == "Poor":
        st.warning(f"**Assessment:** {cause}")
    else:
        st.info(f"**Assessment:** {cause}")

    st.subheader("RF Metrics")
    metrics = pd.DataFrame([{
        "RSRP (dBm)": cell["rsrp"], "RSRQ (dB)": cell["rsrq"], "SINR (dB)": cell["sinr"],
        "CQI": cell["cqi"], "HSR (%)": cell["ho_success_rate"], "Downlink (Mbps)": cell["downlink_mbps"],
    }])
    st.dataframe(metrics, use_container_width=True, hide_index=True)

    if not ran_hour.empty:
        cell_hist = ran_hour[(ran_hour["site_id"] == cell["site_id"]) & (ran_hour["cell_id"] == cell["cell_id"])]
        if not cell_hist.empty:
            st.subheader("Throughput History (last hour)")
            fig = px.line(cell_hist.sort_values("ingested_at"), x="ingested_at", y="downlink_mbps",
                          markers=True, template=PLOTLY_TEMPLATE)
            st.plotly_chart(apply_plotly_style(fig), use_container_width=True)


# ===============================
# PAGE 4 — WEATHER IMPACT
# ===============================
def page_weather_impact():
    render_header()
    st.markdown('<p class="section-title">🌦️ Environmental Impact on Network</p>', unsafe_allow_html=True)

    power_alerts = [a for a in alert_feed if a["category"] == "Power"]
    weather_alerts = [a for a in alert_feed if a["category"] == "Weather"]

    if power_alerts:
        st.markdown("**🔋 Power Alerts (incl. damaged battery)**")
        render_alert_feed(power_alerts, max_items=5)

    if weather_alerts:
        st.markdown("**🌧️ Weather-Related Alerts**")
        render_alert_feed(weather_alerts, max_items=5)

    if weather_latest.empty:
        st.warning("No weather data — start `weather-producer` and `spark-weather`.")
        return

    if ran_weather.empty or ran_weather["weather_temperature_c"].isna().all():
        st.warning("Weather data exists but cannot be matched to RAN sites (check `ran_site_id`).")
        return

    matched = ran_weather.dropna(subset=["weather_temperature_c"])
    heavy_rain = matched[matched["rain_intensity"].isin(["Moderate", "Heavy"])]
    poor_in_rain = heavy_rain[heavy_rain["signal_quality"] == "Poor"]

    w1, w2, w3, w4 = st.columns(4)
    w1.metric("🌡️ Avg Temp", f"{matched['weather_temperature_c'].mean():.1f} °C")
    w2.metric("🌧️ Heavy Rain Cells", len(heavy_rain))
    w3.metric("📶 Poor Signal (Rain)", len(poor_in_rain))
    w4.metric("🔋 Low Battery Sites",
              int(site_insights["battery_charge"].lt(30).sum()) if not site_insights.empty else 0)

    if not poor_in_rain.empty:
        st.error(
            f"⚠️ **Rain impact detected:** {len(poor_in_rain)} cell(s) show poor signal during moderate/heavy rainfall. "
            "Possible rain fade on microwave backhaul or increased atmospheric attenuation."
        )

    failed_batteries = ran_latest[ran_latest["battery_status"].astype(str).str.upper().isin(["DOWN", "FAILED"])]
    if not failed_batteries.empty:
        for site in failed_batteries["site_name"].unique():
            st.error(f"🔋 **Damaged battery alert** — {site}: battery FAILED/DOWN. Generator may be required.")

    c1, c2 = st.columns(2)
    with c1:
        fig = px.scatter(matched, x="weather_temperature_c", y="sinr", color="site_name",
                         symbol="rain_intensity", trendline="ols", template=PLOTLY_TEMPLATE,
                         title="SINR vs Temperature", labels={"weather_temperature_c": "°C", "sinr": "SINR (dB)"})
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)
    with c2:
        fig = px.scatter(matched, x="weather_wind_speed_kmh", y="rsrp", color="site_name",
                         trendline="ols", template=PLOTLY_TEMPLATE,
                         title="RSRP vs Wind", labels={"weather_wind_speed_kmh": "km/h", "rsrp": "RSRP (dBm)"})
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        fig = px.scatter(matched, x="weather_humidity_pct", y="cqi", color="signal_quality",
                         color_discrete_map={"Excellent": "#22c55e", "Good": "#3b82f6", "Poor": "#ef4444"},
                         trendline="ols", template=PLOTLY_TEMPLATE, title="CQI vs Humidity")
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)
    with c4:
        fig = px.box(matched, x="rain_intensity", y="downlink_mbps", color="rain_intensity", points="all",
                     category_orders={"rain_intensity": ["None", "Light", "Moderate", "Heavy"]},
                     template=PLOTLY_TEMPLATE, title="Downlink vs Rain Intensity")
        fig.update_layout(showlegend=False)
        st.plotly_chart(apply_plotly_style(fig), use_container_width=True)

    st.subheader("Site-Level: Network × Weather")
    if not site_insights.empty:
        display = site_insights.copy()
        for col in ["avg_sinr", "avg_rsrp", "avg_cqi", "avg_downlink_mbps", "battery_charge"]:
            if col in display.columns:
                display[col] = display[col].round(2)
        st.dataframe(display, use_container_width=True, hide_index=True)


# ===============================
# NAVIGATION
# ===============================
pg = st.navigation({
    "Monitoring": [
        st.Page(page_network_overview, title="Network Overview", icon="📡", default=True),
        st.Page(page_tower_insights, title="Tower Insights", icon="🗼"),
        st.Page(page_cell_diagnostics, title="Cell Diagnostics", icon="📶"),
        st.Page(page_weather_impact, title="Weather Impact", icon="🌦️"),
    ],
})
pg.run()
