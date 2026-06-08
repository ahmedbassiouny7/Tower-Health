"""
Tower Monitor — Network Operations Center Dashboard
Real-time RAN telemetry from 4 Egyptian tower sites.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlalchemy
from datetime import datetime
import numpy as np
from streamlit_autorefresh import st_autorefresh

# ─── Page Configuration ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tower Monitor NOC",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  [data-testid="stAppViewContainer"] { background: #0b0f1a; }
  [data-testid="stSidebar"] { background: #0d1526; border-right: 1px solid #1e3a5f; }
  .block-container { padding-top: 1.2rem !important; }

  div[data-testid="stMetricValue"] { font-size: 1.7rem; font-weight: 700; }
  div[data-testid="stMetricLabel"] { font-size: 0.72rem; color: #7a9bbf;
                                     letter-spacing: 0.05em; text-transform: uppercase; }
  div[data-testid="stMetricDelta"] { font-size: 0.78rem; }

  .noc-divider { border-top: 1px solid #1e3a5f; margin: 0.8rem 0; }
  [data-testid="stTabs"] button { font-size: 0.85rem; padding: 0.4rem 1rem; }
  .stDataFrame thead th {
    background: #0d1e35 !important; color: #7a9bbf !important; font-size: 0.78rem;
  }
</style>
""", unsafe_allow_html=True)

# ─── Constants ─────────────────────────────────────────────────────────────────
DB_URL = "postgresql://towerhealth:towerhealth@localhost:5432/towerhealth"

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(13,30,53,0.6)",
    font=dict(color="#c0cfe0", size=11),
    margin=dict(l=10, r=10, t=30, b=10),
    xaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", showline=False),
    yaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f"),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
)

SITE_COLORS = {
    "AL_TOWER_01": "#4a9eff",
    "CA_TOWER_02": "#00e096",
    "GZ_TOWER_03": "#ff6b35",
    "KS_TOWER_04": "#c084fc",
}
TECH_COLORS   = {"4G": "#4a9eff", "5G": "#ff6b35"}
HEALTH_COLORS = {"Healthy": "#00e096", "Degraded": "#ffaa00", "Critical": "#ff4444"}

# ─── DB Helpers ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return sqlalchemy.create_engine(DB_URL, pool_pre_ping=True, pool_size=3)


@st.cache_data(ttl=25)
def sql(query: str) -> pd.DataFrame:
    try:
        return pd.read_sql(query, get_engine())
    except Exception as exc:
        st.warning(f"DB query failed: {exc}")
        return pd.DataFrame()


def table_exists(name: str) -> bool:
    df = sql(f"SELECT to_regclass('public.{name}') AS t")
    return not df.empty and df["t"].iloc[0] is not None


# ─── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📡 Tower Monitor NOC")
    st.caption("Egyptian RAN Telemetry · Live Pipeline")
    st.markdown('<div class="noc-divider"></div>', unsafe_allow_html=True)

    time_window = st.selectbox(
        "Time Window",
        ["Last 15 min", "Last 1 hour", "Last 6 hours", "Last 24 hours"],
        index=1,
    )
    MINS = {
        "Last 15 min": 15,
        "Last 1 hour": 60,
        "Last 6 hours": 360,
        "Last 24 hours": 1440,
    }[time_window]

    refresh_s = st.selectbox("Auto-refresh (s)", [15, 30, 60], index=1)

    st.markdown('<div class="noc-divider"></div>', unsafe_allow_html=True)
    if st.button("🔄  Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown('<div class="noc-divider"></div>', unsafe_allow_html=True)
    st.caption(f"🕐  {datetime.now().strftime('%H:%M:%S  ·  %d %b %Y')}")

# Auto-refresh ticker
st_autorefresh(interval=refresh_s * 1000, key="noc_refresh")

# ─── Page Header ───────────────────────────────────────────────────────────────
st.markdown("# 📡  Tower Monitor — Network Operations Center")
st.caption(
    f"4 sites · Alexandria · Cairo · Giza · North Sinai  |  "
    f"Window: **{time_window}**  |  Refresh every **{refresh_s}s**  |  "
    f"As of {datetime.now().strftime('%H:%M:%S')}"
)
st.markdown('<div class="noc-divider"></div>', unsafe_allow_html=True)

# ─── Load Data ─────────────────────────────────────────────────────────────────
HAS_RAN   = table_exists("processed_ran_metrics")
HAS_WX    = table_exists("weather_metrics")
HAS_KAFKA = table_exists("kafka_events")

if HAS_RAN:
    latest_ran = sql("""
        SELECT DISTINCT ON (site_id, cell_id)
            site_id, site_name, vendor, lat, lon,
            cell_id, tech, cell_status, users, downlink_mbps,
            rsrp, rsrq, sinr, cqi, ho_success_rate, signal_quality,
            alert_severity, avg_ru_temp, battery_charge, battery_status,
            ingested_at
        FROM processed_ran_metrics
        ORDER BY site_id, cell_id, ingested_at DESC
    """)

    ran_ts = sql(f"""
        SELECT
            DATE_TRUNC('minute', ingested_at)   AS ts,
            site_name, vendor, tech,
            AVG(sinr)            AS avg_sinr,
            AVG(rsrp)            AS avg_rsrp,
            AVG(rsrq)            AS avg_rsrq,
            AVG(cqi)             AS avg_cqi,
            SUM(users)           AS total_users,
            AVG(downlink_mbps)   AS avg_dl_mbps,
            AVG(ho_success_rate) AS avg_ho
        FROM processed_ran_metrics
        WHERE ingested_at > NOW() - INTERVAL '{MINS} minutes'
        GROUP BY 1, 2, 3, 4
        ORDER BY 1
    """)

    alerts_df = sql(f"""
        SELECT ingested_at, site_name, vendor, tech,
               alert_severity, cell_id, sinr, rsrp, users, downlink_mbps
        FROM processed_ran_metrics
        WHERE alert_severity IN ('CRITICAL','WARNING')
          AND ingested_at > NOW() - INTERVAL '{MINS} minutes'
        ORDER BY ingested_at DESC
        LIMIT 300
    """)
else:
    latest_ran = ran_ts = alerts_df = pd.DataFrame()

if HAS_WX:
    latest_wx = sql("""
        SELECT DISTINCT ON (ran_site_id)
            ran_site_id, ran_site_name, ran_region,
            weather_temperature_c, weather_humidity_pct,
            weather_rainfall_mm, weather_wind_speed_kmh,
            weather_condition, is_raining, rain_intensity, ingested_at
        FROM weather_metrics
        ORDER BY ran_site_id, ingested_at DESC
    """)
    wx_ts = sql(f"""
        SELECT
            DATE_TRUNC('hour', ingested_at)       AS ts,
            ran_site_name,
            AVG(weather_temperature_c)  AS avg_temp,
            AVG(weather_humidity_pct)   AS avg_humidity,
            AVG(weather_rainfall_mm)    AS avg_rain,
            AVG(weather_wind_speed_kmh) AS avg_wind
        FROM weather_metrics
        WHERE ingested_at > NOW() - INTERVAL '{MINS} minutes'
        GROUP BY 1, 2
        ORDER BY 1
    """)
else:
    latest_wx = wx_ts = pd.DataFrame()

# ─── KPI Row ───────────────────────────────────────────────────────────────────
active_sites = latest_ran["site_id"].nunique() if not latest_ran.empty else 0
total_users  = int(latest_ran["users"].sum())   if not latest_ran.empty else 0
avg_sinr     = latest_ran["sinr"].mean()         if not latest_ran.empty else float("nan")
avg_rsrp     = latest_ran["rsrp"].mean()         if not latest_ran.empty else float("nan")
avg_dl       = latest_ran["downlink_mbps"].mean() if not latest_ran.empty else float("nan")
n_crit_kpi   = len(alerts_df[alerts_df["alert_severity"] == "CRITICAL"]) if not alerts_df.empty else 0
avg_bat      = latest_ran["battery_charge"].mean() if not latest_ran.empty else float("nan")

kc = st.columns(7)
kc[0].metric("Active Sites",       f"{active_sites}/4",
             "All Online" if active_sites == 4 else f"{4-active_sites} Offline",
             delta_color="normal" if active_sites == 4 else "inverse")
kc[1].metric("Connected Users",    f"{total_users:,}")
kc[2].metric("Avg SINR",           f"{avg_sinr:.1f} dB"   if not np.isnan(avg_sinr)  else "–",
             "Good" if (not np.isnan(avg_sinr) and avg_sinr >= 10) else "Poor",
             delta_color="normal"  if (not np.isnan(avg_sinr) and avg_sinr >= 10) else "inverse")
kc[3].metric("Avg RSRP",           f"{avg_rsrp:.1f} dBm"  if not np.isnan(avg_rsrp) else "–",
             "OK" if (not np.isnan(avg_rsrp) and avg_rsrp >= -90) else "Weak",
             delta_color="normal"  if (not np.isnan(avg_rsrp) and avg_rsrp >= -90) else "inverse")
kc[4].metric("Avg DL Throughput",  f"{avg_dl:.1f} Mbps"   if not np.isnan(avg_dl)   else "–")
kc[5].metric("Critical Alerts",    n_crit_kpi,
             delta_color="inverse" if n_crit_kpi > 0 else "off")
kc[6].metric("Avg Battery",        f"{avg_bat:.0f}%"       if not np.isnan(avg_bat)  else "–",
             "OK" if (not np.isnan(avg_bat) and avg_bat >= 50) else "Low",
             delta_color="normal"  if (not np.isnan(avg_bat) and avg_bat >= 50) else "inverse")

st.markdown('<div class="noc-divider"></div>', unsafe_allow_html=True)

# ─── Tabs ──────────────────────────────────────────────────────────────────────
(tab_overview, tab_radio, tab_cells,
 tab_alerts, tab_wx, tab_infra, tab_stream) = st.tabs([
    "🗺️ Overview",
    "📶 Radio Performance",
    "📊 Cell Analytics",
    "🚨 Alerts",
    "🌤️ Weather Impact",
    "🔋 Infrastructure",
    "⚡ Stream Health",
])


# ══════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════
with tab_overview:
    if latest_ran.empty:
        st.info("⏳  Waiting for RAN data — pipeline may still be initialising.")
    else:
        site_sum = (
            latest_ran.groupby(["site_id", "site_name", "vendor", "lat", "lon"])
            .agg(
                total_users =("users",          "sum"),
                avg_sinr    =("sinr",           "mean"),
                avg_rsrp    =("rsrp",           "mean"),
                avg_dl      =("downlink_mbps",  "mean"),
                avg_battery =("battery_charge", "mean"),
                n_critical  =("alert_severity", lambda x: (x == "CRITICAL").sum()),
            )
            .reset_index().round(2)
        )

        def _health(row):
            return sum([
                row["avg_sinr"]    >= 10,
                row["avg_rsrp"]    >= -90,
                row["avg_battery"] >= 40,
                row["n_critical"]  == 0,
            ]) / 4 * 100

        site_sum["health_score"]  = site_sum.apply(_health, axis=1)
        site_sum["health_status"] = site_sum["health_score"].apply(
            lambda s: "Healthy" if s >= 75 else ("Degraded" if s >= 50 else "Critical")
        )

        left, right = st.columns([3, 2])

        with left:
            st.markdown("##### 🗺️ Site Map — Egypt")
            fig_map = px.scatter_mapbox(
                site_sum, lat="lat", lon="lon",
                color="health_status", size="total_users", size_max=45,
                hover_name="site_name",
                hover_data={"vendor":True,"total_users":True,"avg_sinr":":.1f",
                            "avg_rsrp":":.1f","avg_battery":":.0f",
                            "lat":False,"lon":False},
                color_discrete_map=HEALTH_COLORS,
                zoom=5, mapbox_style="carto-darkmatter",
            )
            fig_map.update_layout(
                height=360, paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#c0cfe0"), margin=dict(l=0,r=0,t=0,b=0),
            )
            st.plotly_chart(fig_map, use_container_width=True)

        with right:
            st.markdown("##### Signal Quality")
            if "signal_quality" in latest_ran.columns:
                sq = latest_ran["signal_quality"].value_counts()
                fig = go.Figure(go.Pie(
                    labels=sq.index, values=sq.values, hole=0.52,
                    marker_colors=["#00e096","#ffaa00","#ff4444"],
                    textfont=dict(size=11),
                ))
                fig.update_layout(height=165, paper_bgcolor="rgba(0,0,0,0)",
                                   font=dict(color="#c0cfe0"),
                                   legend=dict(bgcolor="rgba(0,0,0,0)",font=dict(size=10)),
                                   margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("##### 4G vs 5G Users")
            if "tech" in latest_ran.columns:
                tu = latest_ran.groupby("tech")["users"].sum()
                fig = go.Figure(go.Pie(
                    labels=tu.index, values=tu.values, hole=0.52,
                    marker_colors=[TECH_COLORS.get(t,"#888") for t in tu.index],
                    textfont=dict(size=11),
                ))
                fig.update_layout(height=165, paper_bgcolor="rgba(0,0,0,0)",
                                   font=dict(color="#c0cfe0"),
                                   legend=dict(bgcolor="rgba(0,0,0,0)",font=dict(size=10)),
                                   margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("##### Site Status Summary")
        disp = site_sum[[
            "site_name","vendor","total_users","avg_sinr","avg_rsrp",
            "avg_dl","avg_battery","n_critical","health_status","health_score",
        ]].copy()
        disp.columns = ["Site","Vendor","Users","SINR (dB)","RSRP (dBm)",
                         "DL Mbps","Battery %","Crit Alerts","Status","Health"]

        def _clr_status(v):
            if v == "Critical":  return "color:#ff4444;font-weight:700"
            if v == "Degraded":  return "color:#ffaa00;font-weight:600"
            return "color:#00e096"

        st.dataframe(
            disp.style
                .map(_clr_status, subset=["Status"])
                .background_gradient(subset=["Health"], cmap="RdYlGn", vmin=0, vmax=100),
            use_container_width=True, hide_index=True,
        )


# ══════════════════════════════════════════════════════════════════════
# TAB 2 — RADIO PERFORMANCE
# ══════════════════════════════════════════════════════════════════════
with tab_radio:
    if ran_ts.empty:
        st.info("⏳  No history yet for the selected time window.")
    else:
        r1, r2 = st.columns(2)

        with r1:
            st.markdown("##### SINR Over Time")
            fig = px.line(ran_ts, x="ts", y="avg_sinr", color="site_name",
                          labels={"ts":"","avg_sinr":"SINR (dB)","site_name":"Site"},
                          color_discrete_map=SITE_COLORS)
            fig.add_hline(y=10, line_dash="dash", line_color="#ffaa00",
                          annotation_text="Threshold 10 dB", annotation_font_size=10)
            fig.update_layout(height=270, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with r2:
            st.markdown("##### RSRP Over Time")
            fig = px.line(ran_ts, x="ts", y="avg_rsrp", color="site_name",
                          labels={"ts":"","avg_rsrp":"RSRP (dBm)","site_name":"Site"},
                          color_discrete_map=SITE_COLORS)
            fig.add_hline(y=-90,  line_dash="dash", line_color="#ffaa00",
                          annotation_text="Weak  −90 dBm", annotation_font_size=10)
            fig.add_hline(y=-110, line_dash="dot",  line_color="#ff4444",
                          annotation_text="Critical −110 dBm", annotation_font_size=10)
            fig.update_layout(height=270, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        r3, r4 = st.columns(2)

        with r3:
            st.markdown("##### Downlink Throughput (Mbps)")
            fig = px.area(ran_ts, x="ts", y="avg_dl_mbps", color="site_name",
                          labels={"ts":"","avg_dl_mbps":"DL Mbps","site_name":"Site"},
                          color_discrete_map=SITE_COLORS)
            fig.update_layout(height=250, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with r4:
            st.markdown("##### Connected Users Over Time")
            fig = px.area(ran_ts, x="ts", y="total_users", color="site_name",
                          labels={"ts":"","total_users":"Users","site_name":"Site"},
                          color_discrete_map=SITE_COLORS)
            fig.update_layout(height=250, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        r5, r6 = st.columns(2)

        with r5:
            st.markdown("##### Signal KPIs by Vendor (current)")
            if not latest_ran.empty:
                v_kpi = (latest_ran.groupby("vendor")
                         .agg(avg_sinr=("sinr","mean"), avg_cqi=("cqi","mean"))
                         .reset_index().round(2))
                fig = go.Figure()
                for col_, color in [("avg_sinr","#4a9eff"), ("avg_cqi","#00e096")]:
                    fig.add_trace(go.Bar(
                        name=col_.replace("avg_","").upper(),
                        x=v_kpi["vendor"], y=v_kpi[col_],
                        marker_color=color,
                    ))
                fig.update_layout(barmode="group", height=250, **CHART_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

        with r6:
            st.markdown("##### Handover Success Rate (%)")
            ho = ran_ts.groupby("site_name")["avg_ho"].mean().reset_index()
            bar_c = ["#00e096" if v >= 90 else "#ffaa00" if v >= 70 else "#ff4444"
                     for v in ho["avg_ho"]]
            fig = go.Figure(go.Bar(
                x=ho["site_name"], y=ho["avg_ho"],
                marker_color=bar_c,
                text=ho["avg_ho"].round(1).astype(str) + "%",
                textposition="outside",
            ))
            fig.update_layout(height=250, yaxis_range=[0, 108], **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════
# TAB 3 — CELL ANALYTICS
# ══════════════════════════════════════════════════════════════════════
with tab_cells:
    if latest_ran.empty:
        st.info("⏳  No cell data available.")
    else:
        c1, c2 = st.columns(2)

        with c1:
            st.markdown("##### SINR: 4G vs 5G per Site")
            d = latest_ran.groupby(["site_name","tech"])["sinr"].mean().reset_index()
            fig = px.bar(d, x="site_name", y="sinr", color="tech", barmode="group",
                         color_discrete_map=TECH_COLORS,
                         labels={"sinr":"Avg SINR (dB)","site_name":"Site","tech":"Tech"})
            fig.update_layout(height=270, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown("##### Users: 4G vs 5G per Site")
            d = latest_ran.groupby(["site_name","tech"])["users"].sum().reset_index()
            fig = px.bar(d, x="site_name", y="users", color="tech", barmode="group",
                         color_discrete_map=TECH_COLORS,
                         labels={"users":"Users","site_name":"Site","tech":"Tech"})
            fig.update_layout(height=270, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        c3, c4 = st.columns(2)

        with c3:
            st.markdown("##### Downlink Throughput by Cell (top 20)")
            top_c = latest_ran.nlargest(20, "downlink_mbps")
            fig = px.bar(top_c, x="cell_id", y="downlink_mbps", color="site_name",
                         color_discrete_map=SITE_COLORS,
                         labels={"downlink_mbps":"DL Mbps","cell_id":"Cell","site_name":"Site"})
            fig.update_layout(height=270, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with c4:
            st.markdown("##### CQI Distribution (4G vs 5G)")
            cqi_df = latest_ran.dropna(subset=["cqi"])
            fig = px.histogram(cqi_df, x="cqi", color="tech", nbins=16, barmode="overlay",
                               opacity=0.72, color_discrete_map=TECH_COLORS,
                               labels={"cqi":"CQI","count":"Count","tech":"Tech"})
            fig.update_layout(height=270, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("##### All Cells — Status")
        cell_tbl = latest_ran[[
            "site_name","cell_id","tech","cell_status","users",
            "downlink_mbps","sinr","rsrp","rsrq","cqi","ho_success_rate","signal_quality",
        ]].copy().round(2)
        cell_tbl.columns = [
            "Site","Cell","Tech","Status","Users","DL Mbps",
            "SINR","RSRP","RSRQ","CQI","HO Success %","Signal Quality",
        ]
        st.dataframe(cell_tbl.sort_values(["Site","Cell"]),
                     use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════
# TAB 4 — ALERTS
# ══════════════════════════════════════════════════════════════════════
with tab_alerts:
    if alerts_df.empty:
        st.success("✅  No critical or warning alerts in the selected time window.")
    else:
        n_c  = len(alerts_df[alerts_df["alert_severity"] == "CRITICAL"])
        n_w  = len(alerts_df[alerts_df["alert_severity"] == "WARNING"])
        n_s  = alerts_df["site_name"].nunique()

        ka1, ka2, ka3 = st.columns(3)
        ka1.metric("Critical", n_c, delta_color="inverse")
        ka2.metric("Warning",  n_w)
        ka3.metric("Affected Sites", n_s)
        st.markdown("")

        la, ra = st.columns([3, 2])

        with la:
            st.markdown("##### Alert Timeline")
            alerts_df["ingested_at"] = pd.to_datetime(alerts_df["ingested_at"])
            fig = px.scatter(
                alerts_df,
                x="ingested_at", y="site_name",
                color="alert_severity",
                symbol="alert_severity",
                hover_data=["tech","sinr","rsrp","users"],
                color_discrete_map={"CRITICAL":"#ff4444","WARNING":"#ffaa00"},
                labels={"ingested_at":"Time","site_name":"Site","alert_severity":"Severity"},
            )
            fig.update_traces(marker_size=9)
            fig.update_layout(height=300, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with ra:
            st.markdown("##### Alerts by Site")
            by_site = (alerts_df.groupby(["site_name","alert_severity"])
                       .size().reset_index(name="count"))
            fig = px.bar(by_site, x="site_name", y="count", color="alert_severity",
                         color_discrete_map={"CRITICAL":"#ff4444","WARNING":"#ffaa00"},
                         labels={"site_name":"Site","count":"Count","alert_severity":"Severity"})
            fig.update_layout(height=300, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("##### Recent Alerts")
        a_tbl = alerts_df[[
            "ingested_at","site_name","vendor","tech",
            "alert_severity","cell_id","sinr","rsrp","users",
        ]].copy().round(2)
        a_tbl.columns = ["Time","Site","Vendor","Tech","Severity","Cell","SINR","RSRP","Users"]

        def _sev(v):
            if v == "CRITICAL":
                return "background:rgba(255,68,68,.15);color:#ff4444;font-weight:700"
            if v == "WARNING":
                return "background:rgba(255,170,0,.12);color:#ffaa00"
            return ""

        st.dataframe(a_tbl.style.map(_sev, subset=["Severity"]),
                     use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════
# TAB 5 — WEATHER IMPACT
# ══════════════════════════════════════════════════════════════════════
with tab_wx:
    if latest_wx.empty:
        st.info("⏳  No weather data yet.")
    else:
        wc1, wc2 = st.columns([3, 2])

        with wc1:
            st.markdown("##### Current Conditions by Site")
            fig = make_subplots(rows=1, cols=2,
                                subplot_titles=("Temperature (°C)", "Humidity (%)"))
            pal = px.colors.qualitative.Set2
            for i, (_, row) in enumerate(latest_wx.iterrows()):
                name = row["ran_site_name"]
                c = pal[i % len(pal)]
                fig.add_trace(go.Bar(x=[name], y=[row["weather_temperature_c"]],
                                     name=name, marker_color=c,
                                     showlegend=(i == 0)), row=1, col=1)
                fig.add_trace(go.Bar(x=[name], y=[row["weather_humidity_pct"]],
                                     name=name, marker_color=c,
                                     showlegend=False), row=1, col=2)
            fig.update_layout(height=290, barmode="group",
                               paper_bgcolor="rgba(0,0,0,0)",
                               plot_bgcolor="rgba(13,30,53,0.6)",
                               font=dict(color="#c0cfe0", size=11),
                               margin=dict(l=10,r=10,t=30,b=10),
                               legend=dict(bgcolor="rgba(0,0,0,0)",font=dict(size=10)))
            st.plotly_chart(fig, use_container_width=True)

        with wc2:
            st.markdown("##### Rain Intensity")
            ri = latest_wx["rain_intensity"].value_counts()
            fig = go.Figure(go.Pie(
                labels=ri.index, values=ri.values, hole=0.48,
                marker_colors=["#4a9eff","#00e096","#ffaa00","#ff4444"],
            ))
            fig.update_layout(height=130, margin=dict(l=0,r=0,t=10,b=0),
                               paper_bgcolor="rgba(0,0,0,0)",
                               font=dict(color="#c0cfe0"))
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("##### Wind Speed (km/h)")
            fig = go.Figure(go.Bar(
                x=latest_wx["ran_site_name"],
                y=latest_wx["weather_wind_speed_kmh"],
                marker_color="#4a9eff",
            ))
            fig.update_layout(height=145, **CHART_LAYOUT,
                               margin=dict(l=10,r=10,t=10,b=10))
            st.plotly_chart(fig, use_container_width=True)

        # Correlation
        if not latest_ran.empty:
            st.markdown("##### Weather → Signal Correlation")
            ran_agg = (latest_ran.groupby("site_id")
                       .agg(avg_sinr=("sinr","mean"), avg_rsrp=("rsrp","mean"))
                       .reset_index())
            merged = ran_agg.merge(
                latest_wx[[
                    "ran_site_id","ran_site_name",
                    "weather_temperature_c","weather_humidity_pct","weather_wind_speed_kmh",
                ]],
                left_on="site_id", right_on="ran_site_id", how="inner",
            )
            if not merged.empty:
                sc1, sc2, sc3 = st.columns(3)
                sct = dict(size=[20]*len(merged), color="ran_site_name",
                           color_discrete_sequence=list(SITE_COLORS.values()))
                with sc1:
                    fig = px.scatter(merged, x="weather_temperature_c", y="avg_sinr",
                                     title="SINR vs Temperature",
                                     labels={"weather_temperature_c":"Temp (°C)",
                                             "avg_sinr":"SINR (dB)","ran_site_name":"Site"},
                                     **sct)
                    fig.update_layout(height=250, **CHART_LAYOUT,
                                       margin=dict(l=10,r=10,t=35,b=10))
                    st.plotly_chart(fig, use_container_width=True)
                with sc2:
                    fig = px.scatter(merged, x="weather_wind_speed_kmh", y="avg_rsrp",
                                     title="RSRP vs Wind Speed",
                                     labels={"weather_wind_speed_kmh":"Wind (km/h)",
                                             "avg_rsrp":"RSRP (dBm)","ran_site_name":"Site"},
                                     **sct)
                    fig.update_layout(height=250, **CHART_LAYOUT,
                                       margin=dict(l=10,r=10,t=35,b=10))
                    st.plotly_chart(fig, use_container_width=True)
                with sc3:
                    fig = px.scatter(merged, x="weather_humidity_pct", y="avg_sinr",
                                     title="SINR vs Humidity",
                                     labels={"weather_humidity_pct":"Humidity (%)",
                                             "avg_sinr":"SINR (dB)","ran_site_name":"Site"},
                                     **sct)
                    fig.update_layout(height=250, **CHART_LAYOUT,
                                       margin=dict(l=10,r=10,t=35,b=10))
                    st.plotly_chart(fig, use_container_width=True)

        st.markdown("##### Weather Table")
        w_tbl = latest_wx[[
            "ran_site_name","ran_region","weather_temperature_c","weather_humidity_pct",
            "weather_rainfall_mm","weather_wind_speed_kmh","weather_condition",
            "is_raining","rain_intensity",
        ]].copy()
        w_tbl.columns = ["Site","Region","Temp °C","Humidity %","Rainfall mm",
                          "Wind km/h","Condition","Raining","Rain Intensity"]
        st.dataframe(w_tbl, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════
# TAB 6 — INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════════════
with tab_infra:
    if latest_ran.empty:
        st.info("⏳  No infrastructure data yet.")
    else:
        infra = (latest_ran.groupby(["site_name","vendor"])
                 .agg(avg_battery=("battery_charge","mean"),
                      avg_ru_temp =("avg_ru_temp",   "mean"))
                 .reset_index().round(1))

        st.markdown("##### Battery Charge Gauges")
        g_cols = st.columns(min(4, len(infra)))

        for i, (_, row) in enumerate(infra.iterrows()):
            val = row["avg_battery"]
            gc  = "#00e096" if val >= 70 else "#ffaa00" if val >= 40 else "#ff4444"
            fig_g = go.Figure(go.Indicator(
                mode="gauge+number",
                value=val,
                number={"suffix":"%","font":{"size":22,"color":"#c0cfe0"}},
                title={"text":
                    f"<b>{row['site_name']}</b><br>"
                    f"<span style='font-size:0.75em;color:#7a9bbf'>{row['vendor']}</span>",
                    "font":{"size":12,"color":"#c0cfe0"}},
                gauge={
                    "axis":      {"range":[0,100],"tickcolor":"#1e3a5f"},
                    "bar":       {"color": gc},
                    "bgcolor":   "rgba(0,0,0,0)",
                    "borderwidth": 0,
                    "steps": [
                        {"range":[0,30],  "color":"rgba(255,68,68,0.18)"},
                        {"range":[30,60], "color":"rgba(255,170,0,0.12)"},
                        {"range":[60,100],"color":"rgba(0,224,150,0.10)"},
                    ],
                    "threshold": {"line":{"color":"#ff4444","width":2},
                                  "thickness":0.75,"value":20},
                },
            ))
            fig_g.update_layout(
                height=200, paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#c0cfe0"), margin=dict(l=10,r=10,t=40,b=10),
            )
            if i < len(g_cols):
                g_cols[i].plotly_chart(fig_g, use_container_width=True)

        ic1, ic2 = st.columns(2)

        with ic1:
            st.markdown("##### RU Temperature by Site")
            bar_c = ["#ff4444" if t >= 60 else "#ffaa00" if t >= 45 else "#4a9eff"
                     for t in infra["avg_ru_temp"]]
            fig = go.Figure(go.Bar(
                x=infra["site_name"], y=infra["avg_ru_temp"],
                marker_color=bar_c,
                text=infra["avg_ru_temp"].astype(str) + "°C",
                textposition="outside",
            ))
            fig.add_hline(y=60, line_dash="dash", line_color="#ff4444",
                          annotation_text="Critical 60°C", annotation_font_size=10)
            fig.add_hline(y=45, line_dash="dash", line_color="#ffaa00",
                          annotation_text="Warning 45°C", annotation_font_size=10)
            fig.update_layout(height=260, yaxis_range=[0,80],
                               yaxis_title="Temperature (°C)", **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with ic2:
            st.markdown("##### Battery Status Distribution")
            bat_st = (latest_ran.groupby(["site_name","battery_status"])
                      .size().reset_index(name="count"))
            if not bat_st.empty:
                fig = px.bar(bat_st, x="site_name", y="count", color="battery_status",
                             labels={"site_name":"Site","count":"Cells",
                                     "battery_status":"Battery Status"})
                fig.update_layout(height=260, **CHART_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("##### Infrastructure Summary per Site")
        i_tbl = (latest_ran.groupby("site_name").agg(
            cells         =("cell_id","nunique"),
            total_users   =("users","sum"),
            avg_battery   =("battery_charge","mean"),
            avg_ru_temp   =("avg_ru_temp","mean"),
            critical_cells=("alert_severity", lambda x: (x=="CRITICAL").sum()),
            vendor        =("vendor","first"),
        ).reset_index().round(2))
        i_tbl.columns = ["Site","Cells","Users","Avg Battery %",
                          "Avg RU Temp °C","Critical Cells","Vendor"]
        st.dataframe(i_tbl, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════
# TAB 7 — STREAM HEALTH
# ══════════════════════════════════════════════════════════════════════
with tab_stream:
    st.markdown("##### Pipeline Counters")
    p1, p2, p3, p4 = st.columns(4)

    if HAS_RAN:
        tot  = sql("SELECT COUNT(*) AS c FROM processed_ran_metrics")
        rec5 = sql("SELECT COUNT(*) AS c FROM processed_ran_metrics "
                   "WHERE ingested_at > NOW() - INTERVAL '5 minutes'")
        p1.metric("RAN Records (total)", f"{int(tot['c'].iloc[0]):,}"  if not tot.empty  else "–")
        p2.metric("RAN (last 5 min)",    f"{int(rec5['c'].iloc[0]):,}" if not rec5.empty else "–")

    if HAS_WX:
        wc = sql("SELECT COUNT(*) AS c FROM weather_metrics")
        p3.metric("Weather Records", f"{int(wc['c'].iloc[0]):,}" if not wc.empty else "–")

    if HAS_KAFKA:
        kc_ = sql("SELECT COUNT(*) AS c FROM kafka_events")
        p4.metric("Kafka Events", f"{int(kc_['c'].iloc[0]):,}" if not kc_.empty else "–")

    if HAS_RAN:
        st.markdown("##### RAN Ingestion Rate (rows/minute)")
        ir = sql(f"""
            SELECT DATE_TRUNC('minute', ingested_at) AS ts, COUNT(*) AS cnt
            FROM processed_ran_metrics
            WHERE ingested_at > NOW() - INTERVAL '{MINS} minutes'
            GROUP BY 1 ORDER BY 1
        """)
        if not ir.empty:
            fig = px.area(ir, x="ts", y="cnt",
                          labels={"ts":"","cnt":"Rows/min"})
            fig.update_layout(height=230, **CHART_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

    if HAS_KAFKA:
        st.markdown("##### Kafka Topic Summary")
        kt = sql("""
            SELECT topic,
                   COUNT(*)                                                        AS total_events,
                   MAX(event_time)                                                 AS last_event,
                   COUNT(*) FILTER (WHERE event_time > NOW() - INTERVAL '5 minutes') AS last_5m
            FROM kafka_events
            GROUP BY topic ORDER BY total_events DESC
        """)
        if not kt.empty:
            st.dataframe(kt, use_container_width=True, hide_index=True)

        st.markdown("##### Latest Raw Events (sample)")
        raw = sql("""
            SELECT topic, event_time, message_key,
                   LEFT(message_value::text, 180) AS payload_preview,
                   ingested_at
            FROM kafka_events
            ORDER BY ingested_at DESC LIMIT 15
        """)
        if not raw.empty:
            st.dataframe(raw, use_container_width=True, hide_index=True)
    else:
        st.info("kafka_events table not found — Kafka landing sink may not be active.")
