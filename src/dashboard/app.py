"""Main Streamlit dashboard application."""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
from PIL import Image
import time
import os
import json
from typing import Optional, Dict

from src.core.database import db_manager
from src.analytics.trajectory_analyzer import TrajectoryAnalyzer
from src.analytics.heatmap_generator import HeatmapGenerator
from src.analytics.zone_analytics import ZoneAnalytics
from src.analytics.performance_metrics import PerformanceMetrics

# Page configuration
st.set_page_config(
    page_title="AGV RTLS Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
        border-left: 3px solid #1f77b4;
    }
    div[data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    .plot-container {
        border: 1px solid #dee2e6;
        border-radius: 5px;
        padding: 10px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

class AGVDashboard:
    """Main dashboard application."""
    
    def __init__(self):
        self.trajectory_analyzer = TrajectoryAnalyzer()
        self.heatmap_generator = HeatmapGenerator()
        self.zone_analytics = ZoneAnalytics()
        self.performance_metrics = PerformanceMetrics()
        self.plant_map = self._load_plant_map()
        self.plant_bounds = self._get_plant_bounds()
    
    def _load_plant_map(self) -> Optional[Image.Image]:
        """Load plant map image."""
        map_path = Path('assets/plant_map.png')
        if map_path.exists():
            return Image.open(map_path)
        return None
    
    def _get_plant_bounds(self) -> Dict:
        """Get plant coordinate bounds."""
        return {
            'xmin': float(os.getenv('PLANT_XMIN', 0)),
            'xmax': float(os.getenv('PLANT_XMAX', 200)),
            'ymin': float(os.getenv('PLANT_YMIN', 0)),
            'ymax': float(os.getenv('PLANT_YMAX', 150))
        }
    
    def render_sidebar(self):
        """Render sidebar with filters."""
        with st.sidebar:
            st.title("🎛️ Control Panel")
            
            # AGV selector
            agvs = db_manager.execute_query(
                "SELECT DISTINCT agv_id FROM agv_registry ORDER BY agv_id"
            )
            agv_list = [agv['agv_id'] for agv in agvs]
            
            selected_agv = st.selectbox(
                "Select AGV",
                options=['All'] + agv_list,
                key='agv_selector'
            )
            
            # Time range selector
            st.subheader("Time Range")
            col1, col2 = st.columns(2)
            
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=datetime.now().date(),
                    key='start_date'
                )
                start_time = st.time_input(
                    "Start Time",
                    value=datetime.now().replace(hour=0, minute=0).time(),
                    key='start_time'
                )
            
            with col2:
                end_date = st.date_input(
                    "End Date",
                    value=datetime.now().date(),
                    key='end_date'
                )
                end_time = st.time_input(
                    "End Time",
                    value=datetime.now().time(),
                    key='end_time'
                )
            
            start_datetime = datetime.combine(start_date, start_time)
            end_datetime = datetime.combine(end_date, end_time)
            
            # Display options
            st.subheader("Display Options")
            show_trajectory = st.checkbox("Show Trajectory", value=True)
            show_heatmap = st.checkbox("Show Heatmap", value=False)
            show_zones = st.checkbox("Show Zones", value=True)
            show_arrows = st.checkbox("Show Direction Arrows", value=False)
            
            # Refresh rate for live view
            st.subheader("Live View")
            auto_refresh = st.checkbox("Auto Refresh", value=False)
            refresh_rate = st.slider(
                "Refresh Rate (seconds)",
                min_value=1,
                max_value=10,
                value=3,
                disabled=not auto_refresh
            )
            
            return {
                'agv': selected_agv,
                'start': start_datetime,
                'end': end_datetime,
                'show_trajectory': show_trajectory,
                'show_heatmap': show_heatmap,
                'show_zones': show_zones,
                'show_arrows': show_arrows,
                'auto_refresh': auto_refresh,
                'refresh_rate': refresh_rate
            }
    
    def render_header_metrics(self):
        """Render header with key metrics."""
        col1, col2, col3, col4, col5 = st.columns(5)
        
        # Get fleet statistics
        stats = self.performance_metrics.get_fleet_stats()
        
        with col1:
            st.metric(
                "Active AGVs",
                f"{stats['active_agvs']} / {stats['total_agvs']}",
                delta=f"{stats['utilization']:.1f}% utilization"
            )
        
        with col2:
            st.metric(
                "Total Distance (24h)",
                f"{stats['total_distance_km']:.1f} km",
                delta=f"{stats['distance_change']:.1f}% vs yesterday"
            )
        
        with col3:
            st.metric(
                "Avg Speed",
                f"{stats['avg_speed_mps']:.2f} m/s",
                delta=f"{stats['speed_change']:.1f}%"
            )
        
        with col4:
            st.metric(
                "Active Zones",
                f"{stats['occupied_zones']} / {stats['total_zones']}",
                delta=None
            )
        
        with col5:
            st.metric(
                "System Health",
                f"{stats['system_health']:.1f}%",
                delta=f"{stats['health_change']:.1f}%"
            )
    
    def render_plant_map(self, filters: Dict):
        """Render main plant map visualization."""
        st.subheader("🗺️ Plant Map View")
        
        # Create plotly figure
        fig = go.Figure()
        
        # Add plant map as background
        if self.plant_map:
            fig.add_layout_image(
                dict(
                    source=self.plant_map,
                    xref="x",
                    yref="y",
                    x=self.plant_bounds['xmin'],
                    y=self.plant_bounds['ymax'],
                    sizex=self.plant_bounds['xmax'] - self.plant_bounds['xmin'],
                    sizey=self.plant_bounds['ymax'] - self.plant_bounds['ymin'],
                    sizing="stretch",
                    opacity=0.7,
                    layer="below"
                )
            )
        
        # Add zones if enabled
        if filters['show_zones']:
            zones = self.zone_analytics.get_zones()
            for _, zone in zones.iterrows():
                if zone['geom']:
                    coords = json.loads(zone['geom'])
                    x_coords = [c[0] for c in coords]
                    y_coords = [c[1] for c in coords]
                    
                    fig.add_trace(go.Scatter(
                        x=x_coords + [x_coords[0]],
                        y=y_coords + [y_coords[0]],
                        mode='lines',
                        name=zone['name'],
                        line=dict(color='rgba(100,100,100,0.3)', width=1),
                        fill='toself',
                        fillcolor='rgba(100,100,100,0.1)',
                        hoverinfo='text',
                        text=f"Zone: {zone['name']}<br>Category: {zone['category']}"
                    ))
        
        # Add AGV trajectories
        if filters['show_trajectory']:
            if filters['agv'] == 'All':
                # Show all AGVs
                agvs = db_manager.execute_query(
                    "SELECT DISTINCT agv_id FROM agv_positions WHERE ts BETWEEN %s AND %s",
                    (filters['start'], filters['end'])
                )
                
                for agv in agvs[:10]:  # Limit to 10 AGVs for performance
                    trajectory = self.trajectory_analyzer.get_trajectory(
                        agv['agv_id'], filters['start'], filters['end']
                    )
                    
                    if not trajectory.empty:
                        fig.add_trace(go.Scatter(
                            x=trajectory['plant_x'],
                            y=trajectory['plant_y'],
                            mode='lines',
                            name=agv['agv_id'],
                            line=dict(width=2),
                            hovertemplate='AGV: %{text}<br>X: %{x:.1f}<br>Y: %{y:.1f}',
                            text=[agv['agv_id']] * len(trajectory)
                        ))
            else:
                # Show selected AGV
                trajectory = self.trajectory_analyzer.get_trajectory(
                    filters['agv'], filters['start'], filters['end']
                )
                
                if not trajectory.empty:
                    # Main trajectory line
                    fig.add_trace(go.Scatter(
                        x=trajectory['plant_x'],
                        y=trajectory['plant_y'],
                        mode='lines+markers',
                        name=filters['agv'],
                        line=dict(width=3, color='blue'),
                        marker=dict(size=4),
                        hovertemplate='Time: %{text}<br>X: %{x:.1f}<br>Y: %{y:.1f}<br>Speed: %{customdata:.1f} m/s',
                        text=trajectory['ts'].dt.strftime('%H:%M:%S'),
                        customdata=trajectory['speed_mps']
                    ))
                    
                    # Add direction arrows
                    if filters['show_arrows'] and len(trajectory) > 10:
                        arrow_indices = np.linspace(0, len(trajectory)-1, 20, dtype=int)
                        for idx in arrow_indices:
                            row = trajectory.iloc[idx]
                            fig.add_annotation(
                                x=row['plant_x'],
                                y=row['plant_y'],
                                ax=row['plant_x'] - 2 * np.cos(np.radians(row['heading_deg'])),
                                ay=row['plant_y'] - 2 * np.sin(np.radians(row['heading_deg'])),
                                xref="x",
                                yref="y",
                                axref="x",
                                ayref="y",
                                showarrow=True,
                                arrowhead=2,
                                arrowsize=1,
                                arrowwidth=2,
                                arrowcolor="rgba(0,0,255,0.5)"
                            )
                    
                    # Start and end markers
                    fig.add_trace(go.Scatter(
                        x=[trajectory.iloc[0]['plant_x']],
                        y=[trajectory.iloc[0]['plant_y']],
                        mode='markers',
                        name='Start',
                        marker=dict(size=15, color='green', symbol='circle'),
                        showlegend=True
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=[trajectory.iloc[-1]['plant_x']],
                        y=[trajectory.iloc[-1]['plant_y']],
                        mode='markers',
                        name='End',
                        marker=dict(size=15, color='red', symbol='square'),
                        showlegend=True
                    ))
        
        # Add heatmap if enabled
        if filters['show_heatmap']:
            heatmap_data = self.heatmap_generator.generate(
                filters['start'], filters['end']
            )
            
            if heatmap_data is not None:
                fig.add_trace(go.Heatmap(
                    z=heatmap_data['z'],
                    x=heatmap_data['x'],
                    y=heatmap_data['y'],
                    colorscale='Jet',
                    opacity=0.5,
                    showscale=True,
                    colorbar=dict(title="Density")
                ))
        
        # Update layout
        fig.update_layout(
            title="AGV Position Tracking",
            xaxis=dict(
                title="Plant X (meters)",
                range=[self.plant_bounds['xmin'], self.plant_bounds['xmax']],
                scaleanchor="y",
                scaleratio=1,
                showgrid=True,
                gridwidth=1,
                gridcolor='LightGray'
            ),
            yaxis=dict(
                title="Plant Y (meters)",
                range=[self.plant_bounds['ymin'], self.plant_bounds['ymax']],
                showgrid=True,
                gridwidth=1,
                gridcolor='LightGray'
            ),
            height=700,
            hovermode='closest',
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_analytics_section(self, filters: Dict):
        """Render analytics section."""
        st.subheader("📊 Analytics")
        
        tab1, tab2, tab3, tab4 = st.tabs([
            "Trajectory Analysis",
            "Zone Analytics",
            "Performance Metrics",
            "Anomaly Detection"
        ])
        
        with tab1:
            self.render_trajectory_analysis(filters)
        
        with tab2:
            self.render_zone_analytics(filters)
        
        with tab3:
            self.render_performance_metrics(filters)
        
        with tab4:
            self.render_anomaly_detection(filters)
    
    def render_trajectory_analysis(self, filters: Dict):
        """Render trajectory analysis."""
        if filters['agv'] == 'All':
            st.info("Please select a specific AGV for trajectory analysis")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Speed profile
            trajectory = self.trajectory_analyzer.get_trajectory(
                filters['agv'], filters['start'], filters['end']
            )
            
            if not trajectory.empty:
                fig = px.line(
                    trajectory,
                    x='ts',
                    y='speed_mps',
                    title='Speed Profile',
                    labels={'speed_mps': 'Speed (m/s)', 'ts': 'Time'}
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
                
                # Statistics
                stats = self.trajectory_analyzer.calculate_stats(trajectory)
                
                st.markdown("**Trajectory Statistics:**")
                col_a, col_b = st.columns(2)
                with col_a:
                    st.metric("Total Distance", f"{stats['total_distance']:.1f} m")
                    st.metric("Avg Speed", f"{stats['avg_speed']:.2f} m/s")
                with col_b:
                    st.metric("Max Speed", f"{stats['max_speed']:.2f} m/s")
                    st.metric("Stop Time", f"{stats['stop_time']:.1f} min")
        
        with col2:
            # Heading distribution
            if not trajectory.empty:
                fig = go.Figure(data=[
                    go.Scatterpolar(
                        r=trajectory['speed_mps'],
                        theta=trajectory['heading_deg'],
                        mode='markers',
                        marker=dict(
                            size=5,
                            color=trajectory['speed_mps'],
                            colorscale='Viridis',
                            showscale=True
                        ),
                        text=trajectory['ts'].dt.strftime('%H:%M:%S')
                    )
                ])
                
                fig.update_layout(
                    title='Heading & Speed Distribution',
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, trajectory['speed_mps'].max()]
                        )
                    ),
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
    
    def render_zone_analytics(self, filters: Dict):
        """Render zone analytics."""
        zone_stats = self.zone_analytics.get_zone_statistics(
            filters['start'], filters['end']
        )
        
        if zone_stats.empty:
            st.warning("No zone data available for selected time range")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Zone occupancy chart
            fig = px.bar(
                zone_stats,
                x='zone_name',
                y='occupancy_time_min',
                title='Zone Occupancy Time (Last 24h)',
                labels={'occupancy_time_min': 'Time (minutes)', 'zone_name': 'Zone'},
                color='category'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Zone transitions heatmap
            transitions = self.zone_analytics.get_zone_transitions(
                filters['start'], filters['end']
            )
            
            if not transitions.empty:
                pivot = transitions.pivot_table(
                    index='from_zone',
                    columns='to_zone',
                    values='count',
                    fill_value=0
                )
                
                fig = px.imshow(
                    pivot,
                    title='Zone Transition Matrix',
                    labels=dict(x="To Zone", y="From Zone", color="Transitions"),
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    
    def render_performance_metrics(self, filters: Dict):
        """Render performance metrics."""
        metrics = self.performance_metrics.calculate_kpis(
            filters['start'], filters['end']
        )
        
        # KPI cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Fleet Efficiency",
                f"{metrics['efficiency']:.1f}%",
                delta=f"{metrics['efficiency_change']:.1f}%"
            )
        
        with col2:
            st.metric(
                "Avg Task Time",
                f"{metrics['avg_task_time']:.1f} min",
                delta=f"{metrics['task_time_change']:.1f}%"
            )
        
        with col3:
            st.metric(
                "Throughput",
                f"{metrics['throughput']:.0f} tasks/hr",
                delta=f"{metrics['throughput_change']:.1f}%"
            )
        
        with col4:
            st.metric(
                "Availability",
                f"{metrics['availability']:.1f}%",
                delta=f"{metrics['availability_change']:.1f}%"
            )
        
        # Time series charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Hourly throughput
            hourly_data = self.performance_metrics.get_hourly_metrics(
                filters['start'], filters['end']
            )
            
            if not hourly_data.empty:
                fig = px.line(
                    hourly_data,
                    x='hour',
                    y='throughput',
                    title='Hourly Throughput',
                    labels={'throughput': 'Tasks/Hour', 'hour': 'Time'}
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Utilization by AGV type
            util_data = self.performance_metrics.get_utilization_by_type(
                filters['start'], filters['end']
            )
            
            if not util_data.empty:
                fig = px.bar(
                    util_data,
                    x='type',
                    y='utilization',
                    title='Utilization by AGV Type',
                    labels={'utilization': 'Utilization (%)', 'type': 'AGV Type'},
                    color='utilization',
                    color_continuous_scale='RdYlGn'
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
    
    def render_anomaly_detection(self, filters: Dict):
        """Render anomaly detection results."""
        anomalies = db_manager.query_dataframe("""
            SELECT 
                event_type,
                severity,
                agv_id,
                zone_id,
                message,
                created_at
            FROM system_events
            WHERE created_at BETWEEN :start AND :end
            AND event_type = 'ANOMALY_DETECTED'
            ORDER BY created_at DESC
            LIMIT 100
        """, {'start': filters['start'], 'end': filters['end']})
        
        if anomalies.empty:
            st.success("No anomalies detected in selected time range")
            return
        
        # Anomaly summary
        col1, col2, col3 = st.columns(3)
        
        with col1:
            severity_counts = anomalies['severity'].value_counts()
            fig = px.pie(
                values=severity_counts.values,
                names=severity_counts.index,
                title='Anomalies by Severity',
                color_discrete_map={
                    'CRITICAL': '#FF0000',
                    'ERROR': '#FF6600',
                    'WARNING': '#FFCC00',
                    'INFO': '#00CC00'
                }
            )
            fig.update_layout(height=250)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            type_counts = anomalies['event_type'].value_counts().head(5)
            fig = px.bar(
                x=type_counts.values,
                y=type_counts.index,
                orientation='h',
                title='Top Anomaly Types',
                labels={'x': 'Count', 'y': 'Type'}
            )
            fig.update_layout(height=250)
            st.plotly_chart(fig, use_container_width=True)
        
        with col3:
            # Timeline
            anomalies['created_at'] = pd.to_datetime(anomalies['created_at'])
            hourly_anomalies = anomalies.set_index('created_at').resample('1H').size()
            
            fig = px.line(
                x=hourly_anomalies.index,
                y=hourly_anomalies.values,
                title='Anomaly Timeline',
                labels={'x': 'Time', 'y': 'Count'}
            )
            fig.update_layout(height=250)
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed anomaly table
        st.subheader("Recent Anomalies")
        st.dataframe(
            anomalies[['created_at', 'severity', 'agv_id', 'zone_id', 'message']].head(20),
            use_container_width=True
        )
    
    def run(self):
        """Run the dashboard application."""
        st.title("🤖 AGV RTLS Dashboard - Production System")
        
        # Render components
        filters = self.render_sidebar()
        self.render_header_metrics()
        
        # Main content
        self.render_plant_map(filters)
        self.render_analytics_section(filters)
        
        # Auto-refresh logic
        if filters['auto_refresh']:
            time.sleep(filters['refresh_rate'])
            st.rerun()

def main():
    """Main entry point."""
    dashboard = AGVDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()