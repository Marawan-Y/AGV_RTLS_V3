"""
Fleet Analytics page - Fleet-wide analysis and metrics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

from src.core.database import db_manager
from src.analytics.performance_metrics import PerformanceMetrics
from src.analytics.zone_analytics import ZoneAnalytics
from src.dashboard.components.filters import FilterComponents
from src.dashboard.components.map_renderer import MapRenderer
from src.dashboard.components.chart_builders import ChartBuilder


st.set_page_config(
    page_title="Fleet Analytics",
    page_icon="📊",
    layout="wide"
)

# Initialize components
filters = FilterComponents()
map_renderer = MapRenderer()
chart_builder = ChartBuilder()
performance_metrics = PerformanceMetrics()
zone_analytics = ZoneAnalytics()


def render_page():
    """Render Fleet Analytics page."""
    
    st.title("📊 Fleet Analytics")
    st.markdown("Comprehensive fleet performance analysis and insights")
    
    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        
        # Time range
        start_time, end_time = filters.render_time_range_selector(
            key_prefix="fleet",
            default_hours=24
        )
        
        # AGV type filter
        agv_types = db_manager.execute_query(
            "SELECT DISTINCT type FROM agv_registry ORDER BY type"
        )
        selected_types = st.multiselect(
            "AGV Types",
            options=[t['type'] for t in agv_types],
            default=[t['type'] for t in agv_types],
            key="fleet_types"
        )
        
        # Zone category filter
        zone_categories = db_manager.execute_query(
            "SELECT DISTINCT category FROM plant_zones WHERE active = TRUE ORDER BY category"
        )
        selected_categories = st.multiselect(
            "Zone Categories",
            options=[c['category'] for c in zone_categories],
            default=[c['category'] for c in zone_categories],
            key="fleet_categories"
        )
        
        # Refresh controls
        refresh_options = filters.render_refresh_controls(
            key_prefix="fleet"
        )
    
    # Main content
    
    # Fleet overview metrics
    st.subheader("Fleet Overview")
    
    fleet_stats = performance_metrics.get_fleet_stats(end_time - start_time)
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            "Active AGVs",
            f"{fleet_stats['active_agvs']} / {fleet_stats['total_agvs']}",
            delta=f"{fleet_stats['utilization']:.1f}%"
        )
    
    with col2:
        st.metric(
            "Total Distance",
            f"{fleet_stats['total_distance_km']:.1f} km",
            delta=f"{fleet_stats['distance_change']:.1f}%"
        )
    
    with col3:
        st.metric(
            "Avg Speed",
            f"{fleet_stats['avg_speed_mps']:.2f} m/s",
            delta=f"{fleet_stats['speed_change']:.1f}%"
        )
    
    with col4:
        st.metric(
            "System Health",
            f"{fleet_stats['system_health']:.1f}%",
            delta=f"{fleet_stats['health_change']:.1f}%"
        )
    
    with col5:
        st.metric(
            "Avg Battery",
            f"{fleet_stats['avg_battery']:.0f}%",
            delta=None
        )
    
    with col6:
        st.metric(
            "Active Zones",
            f"{fleet_stats['occupied_zones']} / {fleet_stats['total_zones']}",
            delta=None
        )
    
    # Tabs for different analyses
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Utilization",
        "Performance",
        "Zone Analysis",
        "Task Analysis",
        "Maintenance"
    ])
    
    with tab1:
        st.subheader("Fleet Utilization Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Utilization by AGV type
            util_data = performance_metrics.get_utilization_by_type(start_time, end_time)
            
            if not util_data.empty and selected_types:
                util_data = util_data[util_data['type'].isin(selected_types)]
                
                fig = px.bar(
                    util_data,
                    x='type',
                    y='utilization',
                    title='Utilization by AGV Type',
                    color='utilization',
                    color_continuous_scale='RdYlGn',
                    labels={'utilization': 'Utilization (%)', 'type': 'AGV Type'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Hourly utilization trend
            hourly_metrics = performance_metrics.get_hourly_metrics(start_time, end_time)
            
            if not hourly_metrics.empty:
                fig = px.line(
                    hourly_metrics,
                    x='hour',
                    y='efficiency',
                    title='Hourly Efficiency Trend',
                    labels={'efficiency': 'Efficiency (%)', 'hour': 'Time'},
                    markers=True
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        # Fleet status breakdown
        st.subheader("Current Fleet Status")
        
        # MySQL-safe latest battery per AGV (no DISTINCT ON, unquoted INTERVAL)
        fleet_status = db_manager.query_dataframe("""
            SELECT 
                r.status,
                COUNT(*) AS count,
                AVG(COALESCE(p.battery_percent, 100)) AS avg_battery
            FROM agv_registry r
            LEFT JOIN (
                SELECT p1.agv_id, p1.battery_percent
                FROM agv_positions p1
                JOIN (
                    SELECT agv_id, MAX(ts) AS max_ts
                    FROM agv_positions
                    WHERE ts >= NOW() - INTERVAL 5 MINUTE
                    GROUP BY agv_id
                ) last ON last.agv_id = p1.agv_id AND last.max_ts = p1.ts
            ) p ON r.agv_id = p.agv_id
            GROUP BY r.status
        """)
        
        if not fleet_status.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(
                    fleet_status,
                    values='count',
                    names='status',
                    title='Fleet Status Distribution',
                    color_discrete_map={
                        'ACTIVE': '#2ca02c',
                        'IDLE': '#ff7f0e',
                        'CHARGING': '#1f77b4',
                        'MAINTENANCE': '#d62728',
                        'ERROR': '#ff0000',
                        'OFFLINE': '#808080'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Battery distribution (latest per-AGV; no DISTINCT ON)
                battery_dist = db_manager.query_dataframe("""
                    SELECT 
                        CASE 
                            WHEN battery_percent >= 80 THEN 'Good (80-100%)'
                            WHEN battery_percent >= 50 THEN 'Medium (50-80%)'
                            WHEN battery_percent >= 20 THEN 'Low (20-50%)'
                            ELSE 'Critical (<20%)'
                        END AS battery_level,
                        COUNT(*) AS count
                    FROM (
                        SELECT p1.agv_id, COALESCE(p1.battery_percent, 100) AS battery_percent
                        FROM agv_positions p1
                        JOIN (
                            SELECT agv_id, MAX(ts) AS max_ts
                            FROM agv_positions
                            WHERE ts >= NOW() - INTERVAL 5 MINUTE
                            GROUP BY agv_id
                        ) last ON last.agv_id = p1.agv_id AND last.max_ts = p1.ts
                    ) p
                    GROUP BY battery_level
                    ORDER BY 
                        CASE battery_level
                            WHEN 'Good (80-100%)' THEN 1
                            WHEN 'Medium (50-80%)' THEN 2
                            WHEN 'Low (20-50%)' THEN 3
                            ELSE 4
                        END
                """)
                
                if not battery_dist.empty:
                    fig = chart_builder.build_battery_status_chart(battery_dist)
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Performance Metrics")
        
        # KPIs
        kpis = performance_metrics.calculate_kpis(start_time, end_time)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            fig = chart_builder.build_fleet_utilization_gauge(kpis['efficiency'])
            fig.update_traces(title={'text': 'Efficiency'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = chart_builder.build_fleet_utilization_gauge(kpis['availability'])
            fig.update_traces(title={'text': 'Availability'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col3:
            st.metric(
                "Throughput",
                f"{kpis['throughput']:.1f} tasks/hr",
                delta=f"{kpis['throughput_change']:.1f}%"
            )
            st.metric(
                "Avg Task Time",
                f"{kpis['avg_task_time']:.1f} min",
                delta=f"{kpis['task_time_change']:.1f}%"
            )
        
        with col4:
            oee = kpis['oee']
            st.metric(
                "OEE Score",
                f"{oee:.1f}%",
                delta=None
            )
            
            # OEE breakdown
            st.progress(kpis['efficiency'] / 100)
            st.caption("Efficiency")
            st.progress(kpis['availability'] / 100)
            st.caption("Availability")
        
        # Performance trends
        st.subheader("Performance Trends")
        
        hourly_data = performance_metrics.get_hourly_metrics(start_time, end_time)
        
        if not hourly_data.empty:
            # Distance and speed trends
            fig = px.line(
                hourly_data,
                x='hour',
                y=['distance_km', 'avg_speed'],
                title='Distance and Speed Trends',
                labels={'value': 'Value', 'hour': 'Time'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Task completion trend
            if 'tasks' in hourly_data.columns:
                fig = px.bar(
                    hourly_data,
                    x='hour',
                    y='tasks',
                    title='Hourly Task Completion',
                    labels={'tasks': 'Tasks Completed', 'hour': 'Time'}
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Zone Analysis")
        
        # Zone statistics
        zone_stats = zone_analytics.get_zone_statistics(start_time, end_time)
        
        if not zone_stats.empty and selected_categories:
            zone_stats = zone_stats[zone_stats['category'].isin(selected_categories)]
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Zone occupancy
                fig = chart_builder.build_zone_occupancy_chart(zone_stats)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Zone utilization
                fig = px.scatter(
                    zone_stats,
                    x='unique_agvs',
                    y='utilization',
                    size='occupancy_time_min',
                    color='category',
                    title='Zone Utilization Analysis',
                    labels={
                        'unique_agvs': 'Unique AGVs',
                        'utilization': 'Utilization (%)',
                        'occupancy_time_min': 'Time (min)'
                    },
                    hover_data=['zone_name']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        # Zone transitions
        st.subheader("Zone Transitions")
        
        transitions = zone_analytics.get_zone_transitions(start_time, end_time)
        
        if not transitions.empty:
            fig = chart_builder.build_zone_transition_matrix(transitions)
            st.plotly_chart(fig, use_container_width=True)
        
        # Bottlenecks
        bottlenecks = zone_analytics.find_bottlenecks(end_time - start_time)
        
        if bottlenecks:
            st.subheader("Detected Bottlenecks")
            
            for bottleneck in bottlenecks[:5]:  # Show top 5
                severity_color = {
                    'HIGH': 'red',
                    'MEDIUM': 'orange',
                    'LOW': 'yellow'
                }.get(bottleneck['severity'], 'gray')
                
                st.markdown(
                    f":{severity_color}[{bottleneck['severity']}] "
                    f"**{bottleneck['zone_name']}** - {bottleneck['type']}"
                )
                
                if bottleneck['type'] == 'OCCUPANCY':
                    st.write(f"Current: {bottleneck['current_agvs']}/{bottleneck['max_agvs']} AGVs")
                elif bottleneck['type'] == 'SPEED':
                    st.write(f"Avg Speed: {bottleneck['avg_speed']:.1f}/{bottleneck['max_speed']:.1f} m/s")
    
    with tab4:
        st.subheader("Task Analysis")
        
        # Task statistics
        task_stats = db_manager.query_dataframe("""
            SELECT 
                status,
                COUNT(*) as count,
                AVG(actual_duration_sec / 60) as avg_duration_min,
                AVG(distance_m) as avg_distance_m
            FROM agv_tasks
            WHERE created_at BETWEEN %s AND %s
            GROUP BY status
        """, (start_time, end_time))
        
        if not task_stats.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Task status distribution
                fig = px.pie(
                    task_stats,
                    values='count',
                    names='status',
                    title='Task Status Distribution'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Average task duration by status
                fig = px.bar(
                    task_stats[task_stats['avg_duration_min'].notna()],
                    x='status',
                    y='avg_duration_min',
                    title='Average Task Duration',
                    labels={'avg_duration_min': 'Duration (min)', 'status': 'Status'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col3:
                # Task completion rate
                total_tasks = task_stats['count'].sum()
                completed_tasks = task_stats[task_stats['status'] == 'COMPLETED']['count'].sum() if 'COMPLETED' in task_stats['status'].values else 0
                completion_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                
                st.metric(
                    "Completion Rate",
                    f"{completion_rate:.1f}%",
                    delta=None
                )
                
                st.metric(
                    "Total Tasks",
                    f"{total_tasks:,}",
                    delta=None
                )
        
        # Task timeline
        recent_tasks = db_manager.query_dataframe("""
            SELECT 
                task_id,
                agv_id,
                task_type,
                status,
                priority,
                started_at,
                completed_at,
                origin_zone_id,
                destination_zone_id
            FROM agv_tasks
            WHERE started_at BETWEEN %s AND %s
            ORDER BY started_at DESC
            LIMIT 50
        """, (start_time, end_time))
        
        if not recent_tasks.empty:
            st.subheader("Recent Task Timeline")
            fig = chart_builder.build_task_timeline(recent_tasks)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab5:
        st.subheader("Maintenance Overview")
        
        # Maintenance status
        maintenance_data = db_manager.query_dataframe("""
            SELECT 
                r.agv_id,
                r.display_name,
                r.type,
                r.total_runtime_hours,
                r.total_distance_km,
                r.maintenance_due_date,
                r.status,
                DATEDIFF(r.maintenance_due_date, NOW()) AS days_until_maintenance
            FROM agv_registry r
            WHERE r.maintenance_due_date IS NOT NULL
            ORDER BY days_until_maintenance
        """)
        
        if not maintenance_data.empty:
            # Upcoming maintenance
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Upcoming Maintenance**")
                
                upcoming = maintenance_data[maintenance_data['days_until_maintenance'] >= 0].head(10)
                
                for _, agv in upcoming.iterrows():
                    days = agv['days_until_maintenance']
                    color = 'red' if days <= 3 else 'orange' if days <= 7 else 'green'
                    st.markdown(
                        f":{color}[●] **{agv['display_name']}** - {days} days"
                    )
            
            with col2:
                st.write("**Overdue Maintenance**")
                
                overdue = maintenance_data[maintenance_data['days_until_maintenance'] < 0]
                
                if not overdue.empty:
                    for _, agv in overdue.iterrows():
                        st.markdown(
                            f":red[⚠] **{agv['display_name']}** - {abs(agv['days_until_maintenance'])} days overdue"
                        )
                else:
                    st.success("No overdue maintenance")
        
        # Runtime distribution
        runtime_dist = db_manager.query_dataframe("""
            SELECT 
                type,
                AVG(total_runtime_hours) AS avg_runtime,
                MAX(total_runtime_hours) AS max_runtime,
                AVG(total_distance_km) AS avg_distance
            FROM agv_registry
            GROUP BY type
        """)
        
        if not runtime_dist.empty:
            st.subheader("Fleet Runtime Statistics")
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    runtime_dist,
                    x='type',
                    y='avg_runtime',
                    title='Average Runtime by Type',
                    labels={'avg_runtime': 'Runtime (hours)', 'type': 'AGV Type'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    runtime_dist,
                    x='type',
                    y='avg_distance',
                    title='Average Distance by Type',
                    labels={'avg_distance': 'Distance (km)', 'type': 'AGV Type'}
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Auto-refresh
    if refresh_options['auto_refresh']:
        import time
        time.sleep(refresh_options['refresh_rate'])
        st.rerun()


if __name__ == "__main__":
    render_page()
