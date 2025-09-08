"""
Live Monitor page - Real-time AGV monitoring.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import numpy as np  # used for speed vectors

from src.core.database import db_manager
from src.dashboard.components.map_renderer import MapRenderer
from src.analytics.anomaly_detector import AnomalyDetector


st.set_page_config(
    page_title="Live Monitor",
    page_icon="🔴",
    layout="wide"
)

# Initialize components
map_renderer = MapRenderer()
anomaly_detector = AnomalyDetector()


def render_page():
    """Render Live Monitor page."""
    st.title("🔴 Live Monitor")
    st.markdown("Real-time AGV fleet monitoring")

    # Sidebar controls
    with st.sidebar:
        st.header("Live Settings")

        # Refresh rate (seconds)
        refresh_rate = st.slider(
            "Refresh Rate (seconds)",
            min_value=1,
            max_value=30,
            value=3,
            key="live_refresh_rate",
        )

        # Display options
        show_trails = st.checkbox("Show Trails", value=True, key="live_trails")
        trail_length = st.slider(
            "Trail Length (seconds)",
            min_value=5,
            max_value=120,
            value=30,
            key="live_trail_length",
            disabled=not show_trails,
        )

        show_zones = st.checkbox("Show Zones", value=True, key="live_zones")
        show_labels = st.checkbox("Show AGV Labels", value=True, key="live_labels")
        show_speed = st.checkbox("Show Speed Vectors", value=False, key="live_speed")

        # Alert settings
        st.subheader("Alert Settings")
        show_alerts = st.checkbox("Show Alerts", value=True, key="live_alerts")
        alert_severities = st.multiselect(
            "Alert Severities",
            options=["CRITICAL", "ERROR", "WARNING", "INFO"],
            default=["CRITICAL", "ERROR"],
            key="live_alert_severities",
            disabled=not show_alerts,
        )

    # --- Data fetch (single pass per run) ---
    current_time = datetime.now()

    # Latest row per AGV within last 10s
    fleet_positions = db_manager.query_dataframe(
        """
        SELECT 
            p1.agv_id,
            p1.ts,
            p1.plant_x,
            p1.plant_y,
            p1.heading_deg,
            p1.speed_mps,
            p1.zone_id,
            p1.battery_percent,
            p1.status,
            r.display_name,
            r.type
        FROM agv_positions p1
        JOIN (
            SELECT agv_id, MAX(ts) AS max_ts
            FROM agv_positions
            WHERE ts >= NOW() - INTERVAL 10 SECOND
            GROUP BY agv_id
        ) last ON p1.agv_id = last.agv_id AND last.max_ts = p1.ts
        JOIN agv_registry r ON p1.agv_id = r.agv_id
        """
    )

    # --- Metrics row ---
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        active_count = len(fleet_positions)
        total_count = db_manager.execute_query(
            "SELECT COUNT(*) AS count FROM agv_registry"
        )[0]["count"]
        st.metric("Active AGVs", f"{active_count}/{total_count}")

    with col2:
        if not fleet_positions.empty and "speed_mps" in fleet_positions.columns:
            avg_speed = float(fleet_positions["speed_mps"].mean())
            st.metric("Avg Speed", f"{avg_speed:.2f} m/s")
        else:
            st.metric("Avg Speed", "0.00 m/s")

    with col3:
        if not fleet_positions.empty and "battery_percent" in fleet_positions.columns:
            avg_battery = float(fleet_positions["battery_percent"].mean())
            st.metric("Avg Battery", f"{avg_battery:.0f}%")
        else:
            st.metric("Avg Battery", "N/A")

    with col4:
        moving = (
            len(fleet_positions[fleet_positions["speed_mps"] > 0.1])
            if (not fleet_positions.empty and "speed_mps" in fleet_positions.columns)
            else 0
        )
        st.metric("Moving", f"{moving}")

    with col5:
        idle = (
            len(fleet_positions[fleet_positions["speed_mps"] <= 0.1])
            if (not fleet_positions.empty and "speed_mps" in fleet_positions.columns)
            else 0
        )
        st.metric("Idle", f"{idle}")

    with col6:
        st.metric("Last Update", current_time.strftime("%H:%M:%S"))

    # --- Map figure (single render per run) ---
    fig = map_renderer.create_base_figure("Live AGV Positions")

    # Zones (GeoJSON in plant coordinates)
    if show_zones:
        fig = map_renderer.add_zones(fig, show_labels=False)

    # Trails (optional)
    if show_trails and not fleet_positions.empty:
        trail_start = current_time - timedelta(seconds=trail_length)
        for agv_id in fleet_positions["agv_id"].unique():
            trail_data = db_manager.query_dataframe(
                """
                SELECT plant_x, plant_y, ts
                FROM agv_positions
                WHERE agv_id = %s
                  AND ts BETWEEN %s AND NOW()
                ORDER BY ts
                """,
                (agv_id, trail_start),
            )
            if not trail_data.empty:
                fig.add_trace(
                    go.Scatter(
                        x=trail_data["plant_x"],
                        y=trail_data["plant_y"],
                        mode="lines",
                        name=f"Trail-{agv_id}",
                        line=dict(color="rgba(100,100,100,0.3)", width=1),
                        showlegend=False,
                        hoverinfo="skip",
                    )
                )

    # Current positions
    if not fleet_positions.empty:
        fig = map_renderer.add_agv_positions(
            fig,
            fleet_positions,
            show_labels=show_labels,
            show_heading=True,
        )

        # Speed vectors (optional)
        if show_speed and "heading_deg" in fleet_positions.columns:
            for _, agv in fleet_positions.iterrows():
                if agv["speed_mps"] > 0.1:
                    vector_length = agv["speed_mps"] * 5.0  # scale factor
                    end_x = agv["plant_x"] + vector_length * np.cos(
                        np.radians(agv["heading_deg"])
                    )
                    end_y = agv["plant_y"] + vector_length * np.sin(
                        np.radians(agv["heading_deg"])
                    )
                    fig.add_annotation(
                        x=agv["plant_x"],
                        y=agv["plant_y"],
                        ax=end_x,
                        ay=end_y,
                        xref="x",
                        yref="y",
                        axref="x",
                        ayref="y",
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=2,
                        arrowcolor="blue",
                        opacity=0.7,
                    )

    # Collision risks overlay
    if not fleet_positions.empty:
        collision_risks = anomaly_detector.detect_collision_risk(fleet_positions)
        for risk in collision_risks:
            if risk["severity"] in alert_severities:
                agv1 = fleet_positions[fleet_positions["agv_id"] == risk["agv1"]].iloc[0]
                agv2 = fleet_positions[fleet_positions["agv_id"] == risk["agv2"]].iloc[0]
                fig.add_trace(
                    go.Scatter(
                        x=[agv1["plant_x"], agv2["plant_x"]],
                        y=[agv1["plant_y"], agv2["plant_y"]],
                        mode="lines",
                        line=dict(
                            color="red" if risk["severity"] == "CRITICAL" else "orange",
                            width=3,
                            dash="dash",
                        ),
                        name="Collision Risk",
                        showlegend=False,
                        hovertext=f"Collision risk: {risk['time_to_collision']:.1f}s",
                    )
                )

    fig.update_layout(height=600)
    # One chart per run (no keys needed, no duplicates within a run)
    st.plotly_chart(fig, use_container_width=True)

    # Alerts (optional)
    if show_alerts:
        st.subheader("Recent Alerts")
        recent_alerts = db_manager.query_dataframe(
            """
            SELECT 
                event_type,
                severity,
                agv_id,
                zone_id,
                message,
                created_at
            FROM system_events
            WHERE created_at >= NOW() - INTERVAL 5 MINUTE
              AND severity IN ({})
            ORDER BY created_at DESC
            LIMIT 10
            """.format(
                ",".join([f"'{s}'" for s in alert_severities])
            )
        )

        if not recent_alerts.empty:
            for _, alert in recent_alerts.iterrows():
                severity_emoji = {
                    "CRITICAL": "🔴",
                    "ERROR": "🟠",
                    "WARNING": "🟡",
                    "INFO": "🔵",
                }.get(alert["severity"], "⚪")

                secs = (current_time - alert["created_at"]).total_seconds()
                time_str = f"{secs:.0f}s ago" if secs < 60 else f"{secs/60:.0f}m ago"

                st.markdown(
                    f"{severity_emoji} **{alert['event_type']}** - "
                    f"{alert['message']} *({time_str})*"
                )
        else:
            st.info("No recent alerts")

    # --- Auto-refresh safely (no infinite loop) ---
    time.sleep(refresh_rate)
    st.rerun()


if __name__ == "__main__":
    render_page()
