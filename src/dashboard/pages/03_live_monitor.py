"""
Live Monitor page - Real-time AGV monitoring.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import asyncio

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
        
        # Refresh rate
        refresh_rate = st.slider(
            "Refresh Rate (seconds)",
            min_value=1,
            max_value=10,
            value=3,
            key="live_refresh_rate"
        )
        
        # Display options
        show_trails = st.checkbox("Show Trails", value=True, key="live_trails")
        trail_length = st.slider(
            "Trail Length (seconds)",
            min_value=5,
            max_value=60,
            value=30,
            key="live_trail_length",
            disabled=not show_trails
        )
        
        show_zones = st.checkbox("Show Zones", value=True, key="live_zones")
        show_labels = st.checkbox("Show AGV Labels", value=True, key="live_labels")
        show_speed = st.checkbox("Show Speed Vectors", value=False, key="live_speed")
        
        # Alert settings
        st.subheader("Alert Settings")
        show_alerts = st.checkbox("Show Alerts", value=True, key="live_alerts")
        alert_severities = st.multiselect(
            "Alert Severities",
            options=['CRITICAL', 'ERROR', 'WARNING', 'INFO'],
            default=['CRITICAL', 'ERROR'],
            key="live_alert_severities",
            disabled=not show_alerts
        )
    
    # Create placeholder for live updates
    map_placeholder = st.empty()
    metrics_placeholder = st.empty()
    alerts_placeholder = st.empty()
    
    # Main update loop
    while True:
        try:
            # Get current fleet positions
            current_time = datetime.now()
            
            fleet_positions = db_manager.query_dataframe("""
                SELECT DISTINCT ON (p.agv_id)
                    p.agv_id,
                    p.ts,
                    p.plant_x,
                    p.plant_y,
                    p.heading_deg,
                    p.speed_mps,
                    p.zone_id,
                    p.battery_percent,
                    p.status,
                    r.display_name,
                    r.type
                FROM agv_positions p
                JOIN agv_registry r ON p.agv_id = r.agv_id
                WHERE p.ts >= NOW() - INTERVAL '10 second'
                ORDER BY p.agv_id, p.ts DESC
            """)
            
            # Update metrics
            with metrics_placeholder.container():
                col1, col2, col3, col4, col5, col6 = st.columns(6)
                
                with col1:
                    active_count = len(fleet_positions)
                    total_count = db_manager.execute_query(
                        "SELECT COUNT(*) as count FROM agv_registry"
                    )[0]['count']
                    st.metric("Active AGVs", f"{active_count}/{total_count}")
                
                with col2:
                    if not fleet_positions.empty:
                        avg_speed = fleet_positions['speed_mps'].mean()
                        st.metric("Avg Speed", f"{avg_speed:.2f} m/s")
                    else:
                        st.metric("Avg Speed", "0.00 m/s")
                
                with col3:
                    if not fleet_positions.empty:
                        avg_battery = fleet_positions['battery_percent'].mean()
                        st.metric("Avg Battery", f"{avg_battery:.0f}%")
                    else:
                        st.metric("Avg Battery", "N/A")
                
                with col4:
                    # Count AGVs in motion
                    if not fleet_positions.empty:
                        moving = len(fleet_positions[fleet_positions['speed_mps'] > 0.1])
                        st.metric("Moving", f"{moving}")
                    else:
                        st.metric("Moving", "0")
                
                with col5:
                    # Count idle AGVs
                    if not fleet_positions.empty:
                        idle = len(fleet_positions[fleet_positions['speed_mps'] <= 0.1])
                        st.metric("Idle", f"{idle}")
                    else:
                        st.metric("Idle", "0")
                
                with col6:
                    # Update timestamp
                    st.metric("Last Update", current_time.strftime("%H:%M:%S"))
            
            # Create live map
            with map_placeholder.container():
                fig = map_renderer.create_base_figure("Live AGV Positions")
                
                # Add zones if enabled
                if show_zones:
                    fig = map_renderer.add_zones(fig, show_labels=False)
                
                # Add trails if enabled
                if show_trails and not fleet_positions.empty:
                    trail_start = current_time - timedelta(seconds=trail_length)
                    
                    for agv_id in fleet_positions['agv_id'].unique():
                        trail_data = db_manager.query_dataframe("""
                            SELECT plant_x, plant_y, ts
                            FROM agv_positions
                            WHERE agv_id = %s
                            AND ts BETWEEN %s AND NOW()
                            ORDER BY ts
                        """, (agv_id, trail_start))
                        
                        if not trail_data.empty:
                            # Add fading trail
                            fig.add_trace(go.Scatter(
                                x=trail_data['plant_x'],
                                y=trail_data['plant_y'],
                                mode='lines',
                                name=f"Trail-{agv_id}",
                                line=dict(
                                    color='rgba(100,100,100,0.3)',
                                    width=1
                                ),
                                showlegend=False,
                                hoverinfo='skip'
                            ))
                
                # Add current positions
                if not fleet_positions.empty:
                    fig = map_renderer.add_agv_positions(
                        fig,
                        fleet_positions,
                        show_labels=show_labels,
                        show_heading=True
                    )
                    
                    # Add speed vectors if enabled
                    if show_speed:
                        for _, agv in fleet_positions.iterrows():
                            if agv['speed_mps'] > 0.1:
                                # Calculate vector endpoint
                                import numpy as np
                                vector_length = agv['speed_mps'] * 5  # Scale factor
                                end_x = agv['plant_x'] + vector_length * np.cos(np.radians(agv['heading_deg']))
                                end_y = agv['plant_y'] + vector_length * np.sin(np.radians(agv['heading_deg']))
                                
                                fig.add_annotation(
                                    x=agv['plant_x'],
                                    y=agv['plant_y'],
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
                                    opacity=0.7
                                )
                
                # Check for collision risks
                if not fleet_positions.empty:
                    collision_risks = anomaly_detector.detect_collision_risk(fleet_positions)
                    
                    for risk in collision_risks:
                        if risk['severity'] in alert_severities:
                            # Get positions of AGVs at risk
                            agv1_pos = fleet_positions[fleet_positions['agv_id'] == risk['agv1']].iloc[0]
                            agv2_pos = fleet_positions[fleet_positions['agv_id'] == risk['agv2']].iloc[0]
                            
                            # Draw warning line between AGVs
                            fig.add_trace(go.Scatter(
                                x=[agv1_pos['plant_x'], agv2_pos['plant_x']],
                                y=[agv1_pos['plant_y'], agv2_pos['plant_y']],
                                mode='lines',
                                line=dict(
                                    color='red' if risk['severity'] == 'CRITICAL' else 'orange',
                                    width=3,
                                    dash='dash'
                                ),
                                name='Collision Risk',
                                showlegend=False,
                                hovertext=f"Collision risk: {risk['time_to_collision']:.1f}s"
                            ))
                
                fig.update_layout(height=600)
                st.plotly_chart(fig, use_container_width=True)
            
            # Show alerts if enabled
            if show_alerts:
                with alerts_placeholder.container():
                    st.subheader("Recent Alerts")
                    
                    recent_alerts = db_manager.query_dataframe("""
                        SELECT 
                            event_type,
                            severity,
                            agv_id,
                            zone_id,
                            message,
                            created_at
                        FROM system_events
                        WHERE created_at >= NOW() - INTERVAL '5 minute'
                        AND severity IN ({})
                        ORDER BY created_at DESC
                        LIMIT 10
                    """.format(','.join([f"'{s}'" for s in alert_severities])))
                    
                    if not recent_alerts.empty:
                        for _, alert in recent_alerts.iterrows():
                            severity_emoji = {
                                'CRITICAL': '🔴',
                                'ERROR': '🟠',
                                'WARNING': '🟡',
                                'INFO': '🔵'
                            }.get(alert['severity'], '⚪')
                            
                            time_ago = (current_time - alert['created_at']).total_seconds()
                            
                            if time_ago < 60:
                                time_str = f"{time_ago:.0f}s ago"
                            else:
                                time_str = f"{time_ago/60:.0f}m ago"
                            
                            st.markdown(
                                f"{severity_emoji} **{alert['event_type']}** - "
                                f"{alert['message']} "
                                f"*({time_str})*"
                            )
                    else:
                        st.info("No recent alerts")
            
            # Sleep before next update
            time.sleep(refresh_rate)
            
        except Exception as e:
            st.error(f"Error updating live monitor: {e}")
            time.sleep(5)


if __name__ == "__main__":
    render_page()