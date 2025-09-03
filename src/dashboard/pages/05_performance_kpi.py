"""
Performance KPI page - Key performance indicators and metrics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from src.core.database import db_manager
from src.analytics.performance_metrics import PerformanceMetrics
from src.dashboard.components.filters import FilterComponents
from src.dashboard.components.chart_builders import ChartBuilder


st.set_page_config(
    page_title="Performance KPIs",
    page_icon="📈",
    layout="wide"
)

# Initialize components
filters = FilterComponents()
chart_builder = ChartBuilder()
performance_metrics = PerformanceMetrics()


def render_page():
    """Render Performance KPI page."""
    
    st.title("📈 Performance KPIs")
    st.markdown("Monitor key performance indicators and operational metrics")
    
    # Sidebar filters
    with st.sidebar:
        st.header("KPI Settings")
        
        # Time range
        time_range = st.selectbox(
            "Time Range",
            options=["Today", "Yesterday", "Last 7 Days", "Last 30 Days", "Custom"],
            index=0,
            key="kpi_time_range"
        )
        
        if time_range == "Custom":
            start_date = st.date_input("Start Date", key="kpi_start_date")
            end_date = st.date_input("End Date", key="kpi_end_date")
            start_time = datetime.combine(start_date, datetime.min.time())
            end_time = datetime.combine(end_date, datetime.max.time())
        else:
            end_time = datetime.now()
            if time_range == "Today":
                start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
            elif time_range == "Yesterday":
                start_time = (end_time - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_time = start_time + timedelta(days=1)
            elif time_range == "Last 7 Days":
                start_time = end_time - timedelta(days=7)
            else:  # Last 30 Days
                start_time = end_time - timedelta(days=30)
        
        # Comparison period
        enable_comparison = st.checkbox("Enable Comparison", value=True)
        
        if enable_comparison:
            comparison_period = st.selectbox(
                "Compare With",
                options=["Previous Period", "Same Period Last Week", "Same Period Last Month"],
                key="kpi_comparison"
            )
        
        # KPI categories
        st.subheader("KPI Categories")
        show_efficiency = st.checkbox("Efficiency", value=True)
        show_productivity = st.checkbox("Productivity", value=True)
        show_quality = st.checkbox("Quality", value=True)
        show_safety = st.checkbox("Safety", value=True)
        
        # Refresh
        auto_refresh = st.checkbox("Auto Refresh", value=False)
        if auto_refresh:
            refresh_rate = st.slider("Refresh Rate (seconds)", 5, 60, 30)
    
    # Calculate KPIs
    kpis = performance_metrics.calculate_kpis(start_time, end_time)
    
    # Main KPI cards
    st.subheader("Overall Performance")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # OEE
        oee = kpis['oee']
        st.metric(
            "Overall Equipment Effectiveness (OEE)",
            f"{oee:.1f}%",
            delta=f"{kpis.get('oee_change', 0):.1f}%" if enable_comparison else None
        )
        
        # OEE components
        with st.expander("OEE Breakdown"):
            st.progress(kpis['availability'] / 100)
            st.caption(f"Availability: {kpis['availability']:.1f}%")
            
            st.progress(kpis['efficiency'] / 100)
            st.caption(f"Performance: {kpis['efficiency']:.1f}%")
            
            quality = 95  # Placeholder for quality metric
            st.progress(quality / 100)
            st.caption(f"Quality: {quality:.1f}%")
    
    with col2:
        # Utilization gauge
        fig = chart_builder.build_fleet_utilization_gauge(kpis['efficiency'])
        st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        # Availability gauge
        fig = chart_builder.build_fleet_utilization_gauge(kpis['availability'])
        fig.update_traces(title={'text': 'Availability'})
        st.plotly_chart(fig, use_container_width=True)
    
    with col4:
        # Throughput
        st.metric(
            "Throughput",
            f"{kpis['throughput']:.1f} tasks/hr",
            delta=f"{kpis['throughput_change']:.1f}%" if enable_comparison else None
        )
        
        st.metric(
            "Avg Task Time",
            f"{kpis['avg_task_time']:.1f} min",
            delta=f"{kpis['task_time_change']:.1f}%" if enable_comparison else None,
            delta_color="inverse"
        )
    
    # Category tabs
    tabs = []
    if show_efficiency:
        tabs.append("Efficiency")
    if show_productivity:
        tabs.append("Productivity")
    if show_quality:
        tabs.append("Quality")
    if show_safety:
        tabs.append("Safety")
    
    if tabs:
        tab_objects = st.tabs(tabs)
        tab_index = 0
        
        if show_efficiency and tab_index < len(tab_objects):
            with tab_objects[tab_index]:
                render_efficiency_kpis(start_time, end_time, enable_comparison)
            tab_index += 1
        
        if show_productivity and tab_index < len(tab_objects):
            with tab_objects[tab_index]:
                render_productivity_kpis(start_time, end_time, enable_comparison)
            tab_index += 1
        
        if show_quality and tab_index < len(tab_objects):
            with tab_objects[tab_index]:
                render_quality_kpis(start_time, end_time, enable_comparison)
            tab_index += 1
        
        if show_safety and tab_index < len(tab_objects):
            with tab_objects[tab_index]:
                render_safety_kpis(start_time, end_time, enable_comparison)
    
    # Trend analysis
    st.subheader("Trend Analysis")
    
    # Get hourly metrics for trend
    hourly_data = performance_metrics.get_hourly_metrics(start_time, end_time)
    
    if not hourly_data.empty:
        # Multi-metric trend chart
        metrics_to_plot = ['efficiency', 'active_agvs', 'distance_km', 'tasks']
        
        fig = go.Figure()
        
        for metric in metrics_to_plot:
            if metric in hourly_data.columns:
                fig.add_trace(go.Scatter(
                    x=hourly_data['hour'],
                    y=hourly_data[metric],
                    mode='lines+markers',
                    name=metric.replace('_', ' ').title(),
                    yaxis='y' if metric in ['efficiency', 'active_agvs'] else 'y2'
                ))
        
        fig.update_layout(
            title="KPI Trends",
            xaxis=dict(title="Time"),
            yaxis=dict(title="Count / Percentage", side="left"),
            yaxis2=dict(title="Distance / Tasks", side="right", overlaying="y"),
            hovermode='x unified',
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Auto-refresh
    if auto_refresh:
        import time
        time.sleep(refresh_rate)
        st.rerun()


def render_efficiency_kpis(start_time, end_time, show_comparison):
    """Render efficiency KPIs."""
    
    st.subheader("Efficiency Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate efficiency metrics
    efficiency_data = db_manager.query_dataframe("""
        SELECT 
            SUM(moving_time_sec) / (SUM(moving_time_sec) + SUM(idle_time_sec)) * 100 as motion_efficiency,
            SUM(total_distance_m) / 1000 as total_distance_km,
            AVG(avg_speed_mps) as avg_speed,
            SUM(idle_time_sec) / 3600 as total_idle_hours
        FROM agv_analytics_hourly
        WHERE hour_start BETWEEN %s AND %s
    """, (start_time, end_time))
    
    if not efficiency_data.empty:
        with col1:
            st.metric(
                "Motion Efficiency",
                f"{efficiency_data['motion_efficiency'].iloc[0]:.1f}%",
                delta=None
            )
        
        with col2:
            st.metric(
                "Total Distance",
                f"{efficiency_data['total_distance_km'].iloc[0]:.1f} km",
                delta=None
            )
        
        with col3:
            st.metric(
                "Average Speed",
                f"{efficiency_data['avg_speed'].iloc[0]:.2f} m/s",
                delta=None
            )
        
        with col4:
            st.metric(
                "Idle Time",
                f"{efficiency_data['total_idle_hours'].iloc[0]:.1f} hours",
                delta=None,
                delta_color="inverse"
            )
    
    # Efficiency by AGV type
    type_efficiency = db_manager.query_dataframe("""
        SELECT 
            r.type,
            COUNT(DISTINCT a.agv_id) as active_agvs,
            SUM(a.moving_time_sec) / (SUM(a.moving_time_sec) + SUM(a.idle_time_sec)) * 100 as efficiency
        FROM agv_registry r
        LEFT JOIN agv_analytics_hourly a ON r.agv_id = a.agv_id
            AND a.hour_start BETWEEN %s AND %s
        GROUP BY r.type
    """, (start_time, end_time))
    
    if not type_efficiency.empty:
        fig = px.bar(
            type_efficiency,
            x='type',
            y='efficiency',
            title='Efficiency by AGV Type',
            labels={'efficiency': 'Efficiency (%)', 'type': 'AGV Type'},
            color='efficiency',
            color_continuous_scale='RdYlGn'
        )
        st.plotly_chart(fig, use_container_width=True)


def render_productivity_kpis(start_time, end_time, show_comparison):
    """Render productivity KPIs."""
    
    st.subheader("Productivity Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate productivity metrics
    productivity_data = db_manager.query_dataframe("""
        SELECT 
            COUNT(*) as total_tasks,
            SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed_tasks,
            AVG(CASE WHEN status = 'COMPLETED' THEN actual_duration_sec / 60 END) as avg_completion_time,
            SUM(CASE WHEN status = 'COMPLETED' THEN distance_m END) / 1000 as task_distance_km
        FROM agv_tasks
        WHERE created_at BETWEEN %s AND %s
    """, (start_time, end_time))
    
    if not productivity_data.empty:
        with col1:
            st.metric(
                "Total Tasks",
                f"{productivity_data['total_tasks'].iloc[0]:,}",
                delta=None
            )
        
        with col2:
            completion_rate = (productivity_data['completed_tasks'].iloc[0] / 
                             productivity_data['total_tasks'].iloc[0] * 100) if productivity_data['total_tasks'].iloc[0] > 0 else 0
            st.metric(
                "Completion Rate",
                f"{completion_rate:.1f}%",
                delta=None
            )
        
        with col3:
            st.metric(
                "Avg Completion Time",
                f"{productivity_data['avg_completion_time'].iloc[0]:.1f} min",
                delta=None
            )
        
        with col4:
            st.metric(
                "Task Distance",
                f"{productivity_data['task_distance_km'].iloc[0]:.1f} km",
                delta=None
            )
    
    # Tasks by priority
    priority_tasks = db_manager.query_dataframe("""
        SELECT 
            CASE 
                WHEN priority >= 8 THEN 'High'
                WHEN priority >= 4 THEN 'Medium'
                ELSE 'Low'
            END as priority_level,
            COUNT(*) as count,
            AVG(CASE WHEN status = 'COMPLETED' THEN actual_duration_sec / 60 END) as avg_time
        FROM agv_tasks
        WHERE created_at BETWEEN %s AND %s
        GROUP BY priority_level
    """, (start_time, end_time))
    
    if not priority_tasks.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                priority_tasks,
                values='count',
                names='priority_level',
                title='Tasks by Priority'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                priority_tasks,
                x='priority_level',
                y='avg_time',
                title='Average Time by Priority',
                labels={'avg_time': 'Avg Time (min)', 'priority_level': 'Priority'}
            )
            st.plotly_chart(fig, use_container_width=True)


def render_quality_kpis(start_time, end_time, show_comparison):
    """Render quality KPIs."""
    
    st.subheader("Quality Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate quality metrics
    quality_data = db_manager.query_dataframe("""
        SELECT 
            AVG(quality) * 100 as signal_quality,
            COUNT(CASE WHEN quality < 0.5 THEN 1 END) as low_quality_count,
            COUNT(*) as total_samples
        FROM agv_positions
        WHERE ts BETWEEN %s AND %s
    """, (start_time, end_time))
    
    if not quality_data.empty:
        with col1:
            st.metric(
                "Signal Quality",
                f"{quality_data['signal_quality'].iloc[0]:.1f}%",
                delta=None
            )
        
        with col2:
            error_rate = (quality_data['low_quality_count'].iloc[0] / 
                         quality_data['total_samples'].iloc[0] * 100) if quality_data['total_samples'].iloc[0] > 0 else 0
            st.metric(
                "Error Rate",
                f"{error_rate:.2f}%",
                delta=None,
                delta_color="inverse"
            )
        
        with col3:
            st.metric(
                "Low Quality Samples",
                f"{quality_data['low_quality_count'].iloc[0]:,}",
                delta=None
            )
        
        with col4:
            st.metric(
                "Total Samples",
                f"{quality_data['total_samples'].iloc[0]:,}",
                delta=None
            )


def render_safety_kpis(start_time, end_time, show_comparison):
    """Render safety KPIs."""
    
    st.subheader("Safety Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate safety metrics
    safety_data = db_manager.query_dataframe("""
        SELECT 
            COUNT(CASE WHEN event_type = 'COLLISION_RISK' THEN 1 END) as collision_risks,
            COUNT(CASE WHEN event_type = 'ZONE_VIOLATION' THEN 1 END) as zone_violations,
            COUNT(CASE WHEN event_type = 'SPEED_VIOLATION' THEN 1 END) as speed_violations,
            COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_events
        FROM system_events
        WHERE created_at BETWEEN %s AND %s
    """, (start_time, end_time))
    
    if not safety_data.empty:
        with col1:
            st.metric(
                "Collision Risks",
                f"{safety_data['collision_risks'].iloc[0]}",
                delta=None,
                delta_color="inverse"
            )
        
        with col2:
            st.metric(
                "Zone Violations",
                f"{safety_data['zone_violations'].iloc[0]}",
                delta=None,
                delta_color="inverse"
            )
        
        with col3:
            st.metric(
                "Speed Violations",
                f"{safety_data['speed_violations'].iloc[0]}",
                delta=None,
                delta_color="inverse"
            )
        
        with col4:
            st.metric(
                "Critical Events",
                f"{safety_data['critical_events'].iloc[0]}",
                delta=None,
                delta_color="inverse"
            )
    
    # Safety trend
    safety_trend = db_manager.query_dataframe("""
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as event_count,
            event_type
        FROM system_events
        WHERE created_at BETWEEN %s AND %s
        AND event_type IN ('COLLISION_RISK', 'ZONE_VIOLATION', 'SPEED_VIOLATION')
        GROUP BY DATE(created_at), event_type
        ORDER BY date
    """, (start_time, end_time))
    
    if not safety_trend.empty:
        fig = px.line(
            safety_trend,
            x='date',
            y='event_count',
            color='event_type',
            title='Safety Events Trend',
            labels={'event_count': 'Event Count', 'date': 'Date'}
        )
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    render_page()