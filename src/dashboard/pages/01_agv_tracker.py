"""
AGV Tracker page - Individual AGV tracking and analysis.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import time
from datetime import datetime, timedelta

from src.core.database import db_manager
from src.analytics.trajectory_analyzer import TrajectoryAnalyzer
from src.dashboard.components.filters import FilterComponents
from src.dashboard.components.map_renderer import MapRenderer
from src.dashboard.components.chart_builders import ChartBuilder


st.set_page_config(
    page_title="AGV Tracker",
    page_icon="🤖",
    layout="wide"
)

# Initialize components
filters = FilterComponents()
map_renderer = MapRenderer()
chart_builder = ChartBuilder()
trajectory_analyzer = TrajectoryAnalyzer()


def render_page():
    """Render AGV Tracker page."""
    
    st.title("🤖 AGV Tracker")
    st.markdown("Track and analyze individual AGV movements")
    
    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        
        # AGV selector
        selected_agv = filters.render_agv_selector(
            key="tracker_agv",
            include_all=False
        )
        
        # Time range
        start_time, end_time = filters.render_time_range_selector(
            key_prefix="tracker"
        )
        
        # Display options
        display_options = filters.render_display_options(
            key_prefix="tracker"
        )
        
        # Refresh controls
        refresh_options = filters.render_refresh_controls(
            key_prefix="tracker"
        )
    
    # Main content
    if selected_agv and selected_agv != 'All':
        # Get AGV info
        agv_info = db_manager.execute_query("""
            SELECT 
                r.*,
                p.plant_x,
                p.plant_y,
                p.speed_mps,
                p.zone_id,
                p.battery_percent,
                p.ts as last_update
            FROM agv_registry r
            LEFT JOIN (
                SELECT * FROM agv_positions
                WHERE agv_id = %s
                ORDER BY ts DESC
                LIMIT 1
            ) p ON r.agv_id = p.agv_id
            WHERE r.agv_id = %s
        """, (selected_agv, selected_agv))
        
        if agv_info:
            agv_info = agv_info[0]
            
            # Header metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    "Status",
                    agv_info.get('status', 'UNKNOWN'),
                    delta=None
                )
            
            with col2:
                st.metric(
                    "Current Speed",
                    f"{agv_info.get('speed_mps', 0):.1f} m/s",
                    delta=None
                )
            
            with col3:
                st.metric(
                    "Battery",
                    f"{agv_info.get('battery_percent', 0):.0f}%",
                    delta=None
                )
            
            with col4:
                st.metric(
                    "Current Zone",
                    agv_info.get('zone_id', 'Unknown'),
                    delta=None
                )
            
            with col5:
                last_update = agv_info.get('last_update')
                if last_update:
                    seconds_ago = (datetime.now() - last_update).total_seconds()
                    st.metric(
                        "Last Update",
                        f"{seconds_ago:.0f}s ago",
                        delta=None
                    )
            
            # Get trajectory data
            trajectory = trajectory_analyzer.get_trajectory(
                selected_agv, start_time, end_time
            )
            
            # Main map view
            st.subheader("Trajectory Map")
            
            if not trajectory.empty:
                # Create map
                fig = map_renderer.create_base_figure(f"AGV {selected_agv} Trajectory")
                
                # Add zones if enabled
                if display_options['show_zones']:
                    fig = map_renderer.add_zones(fig)
                
                # Add trajectory
                fig = map_renderer.add_trajectory(
                    fig, 
                    trajectory,
                    agv_id=selected_agv,
                    show_arrows=display_options['show_arrows']
                )
                
                # Add heatmap if enabled
                if display_options['show_heatmap']:
                    from src.analytics.heatmap_generator import HeatmapGenerator
                    heatmap_gen = HeatmapGenerator()
                    heatmap_data = heatmap_gen.generate_trajectory_heatmap(
                        selected_agv,
                        end_time - start_time
                    )
                    if heatmap_data:
                        fig = map_renderer.add_heatmap_overlay(fig, heatmap_data)
                
                # Add current position
                if agv_info.get('plant_x'):
                    current_pos = pd.DataFrame([{
                        'agv_id': selected_agv,
                        'plant_x': agv_info['plant_x'],
                        'plant_y': agv_info['plant_y'],
                        'heading_deg': agv_info.get('heading_deg', 0),
                        'speed_mps': agv_info.get('speed_mps', 0),
                        'battery_percent': agv_info.get('battery_percent', 100)
                    }])
                    fig = map_renderer.add_agv_positions(fig, current_pos)
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(f"No trajectory data available for {selected_agv} in selected time range")
            
            # Analytics section
            st.subheader("Analytics")
            
            tab1, tab2, tab3, tab4 = st.tabs([
                "Trajectory Stats",
                "Speed Analysis",
                "Zone Analysis",
                "Stops & Events"
            ])
            
            with tab1:
                if not trajectory.empty:
                    # Calculate statistics
                    stats = trajectory_analyzer.calculate_stats(trajectory)
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Distance", f"{stats['total_distance']:.1f} m")
                        st.metric("Duration", f"{stats['duration_min']:.1f} min")
                    
                    with col2:
                        st.metric("Avg Speed", f"{stats['avg_speed']:.2f} m/s")
                        st.metric("Max Speed", f"{stats['max_speed']:.2f} m/s")
                    
                    with col3:
                        st.metric("Stop Time", f"{stats['stop_time']:.1f} min")
                        st.metric("Stop %", f"{stats['stop_percentage']:.1f}%")
                    
                    with col4:
                        st.metric("Data Points", f"{stats['total_points']:,}")
                        st.metric("Zones Visited", stats['unique_zones'])
                    
                    # Path efficiency
                    efficiency = trajectory_analyzer.calculate_path_efficiency(trajectory)
                    st.metric("Path Efficiency", f"{efficiency*100:.1f}%")
            
            with tab2:
                if not trajectory.empty:
                    # Speed profile chart
                    speed_fig = chart_builder.build_speed_profile(trajectory)
                    st.plotly_chart(speed_fig, use_container_width=True)
                    
                    # Speed distribution
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        speed_hist = px.histogram(
                            trajectory,
                            x='speed_mps',
                            nbins=30,
                            title="Speed Distribution",
                            labels={'speed_mps': 'Speed (m/s)', 'count': 'Frequency'}
                        )
                        st.plotly_chart(speed_hist, use_container_width=True)
                    
                    with col2:
                        # Heading distribution
                        if 'heading_deg' in trajectory.columns:
                            heading_fig = go.Figure(go.Scatterpolar(
                                r=trajectory['speed_mps'],
                                theta=trajectory['heading_deg'],
                                mode='markers',
                                marker=dict(
                                    size=5,
                                    color=trajectory['speed_mps'],
                                    colorscale='Viridis',
                                    showscale=True
                                ),
                                text=trajectory.index
                            ))
                            heading_fig.update_layout(
                                title="Heading & Speed",
                                polar=dict(
                                    radialaxis=dict(
                                        visible=True,
                                        range=[0, trajectory['speed_mps'].max()]
                                    )
                                ),
                                height=400
                            )
                            st.plotly_chart(heading_fig, use_container_width=True)
            
            with tab3:
                # Zone dwell time
                zone_dwell = db_manager.query_dataframe("""
                    SELECT 
                        z.name as zone_name,
                        z.category,
                        COUNT(*) / 3.0 / 60 as dwell_time_min
                    FROM agv_positions p
                    JOIN plant_zones z ON p.zone_id = z.zone_id
                    WHERE p.agv_id = %s
                    AND p.ts BETWEEN %s AND %s
                    GROUP BY z.zone_id, z.name, z.category
                    ORDER BY dwell_time_min DESC
                """, (selected_agv, start_time, end_time))
                
                if not zone_dwell.empty:
                    zone_chart = chart_builder.build_zone_occupancy_chart(zone_dwell)
                    st.plotly_chart(zone_chart, use_container_width=True)
                else:
                    st.info("No zone data available for selected time range")
            
            with tab4:
                # Detect stops
                if not trajectory.empty:
                    stops = trajectory_analyzer.detect_stops(trajectory)
                    
                    if stops:
                        st.write(f"Found {len(stops)} stops")
                        
                        # Display stops table
                        stops_df = pd.DataFrame(stops)
                        st.dataframe(stops_df, use_container_width=True)
                    else:
                        st.info("No stops detected in selected time range")
                
                # Recent events
                st.subheader("Recent Events")
                
                events = db_manager.query_dataframe("""
                    SELECT 
                        event_type,
                        severity,
                        message,
                        created_at
                    FROM system_events
                    WHERE agv_id = %s
                    AND created_at BETWEEN %s AND %s
                    ORDER BY created_at DESC
                    LIMIT 20
                """, (selected_agv, start_time, end_time))
                
                if not events.empty:
                    st.dataframe(events, use_container_width=True)
                else:
                    st.info("No events in selected time range")
        else:
            st.error(f"AGV {selected_agv} not found")
    else:
        st.info("Please select an AGV from the sidebar")
    
    # Auto-refresh
    if refresh_options['auto_refresh']:
        time.sleep(refresh_options['refresh_rate'])
        st.rerun()


if __name__ == "__main__":
    render_page()