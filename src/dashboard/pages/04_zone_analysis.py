"""
Zone Analysis page - Detailed zone utilization and optimization.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from src.core.database import db_manager
from src.analytics.zone_analytics import ZoneAnalytics
from src.dashboard.components.filters import FilterComponents
from src.dashboard.components.map_renderer import MapRenderer
from src.dashboard.components.chart_builders import ChartBuilder


st.set_page_config(
    page_title="Zone Analysis",
    page_icon="🏭",
    layout="wide"
)

# Initialize components
filters = FilterComponents()
map_renderer = MapRenderer()
chart_builder = ChartBuilder()
zone_analytics = ZoneAnalytics()


def render_page():
    """Render Zone Analysis page."""
    
    st.title("🏭 Zone Analysis")
    st.markdown("Analyze zone utilization, transitions, and optimization opportunities")
    
    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        
        # Zone selector
        selected_zone = filters.render_zone_selector(
            key="zone_analysis",
            include_all=True
        )
        
        # Time range
        start_time, end_time = filters.render_time_range_selector(
            key_prefix="zone",
            default_hours=24
        )
        
        # Analysis options
        st.subheader("Analysis Options")
        show_predictions = st.checkbox("Show Predictions", value=False)
        show_recommendations = st.checkbox("Show Recommendations", value=True)
        
        # Refresh controls
        refresh_options = filters.render_refresh_controls(
            key_prefix="zone"
        )
    
    # Main content
    
    # Zone overview metrics
    if selected_zone:
        zone_info = zone_analytics.zone_manager.get_zone_info(selected_zone)
        
        if zone_info:
            st.subheader(f"Zone: {zone_info['name']}")
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Category", zone_info['category'])
            
            with col2:
                st.metric("Type", zone_info['zone_type'])
            
            with col3:
                st.metric("Max AGVs", zone_info['max_agvs'])
            
            with col4:
                st.metric("Max Speed", f"{zone_info['max_speed_mps']:.1f} m/s")
            
            with col5:
                st.metric("Priority", zone_info['priority'])
    
    # Tabs for different analyses
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Utilization",
        "Transitions",
        "Bottlenecks",
        "Optimization",
        "Predictions"
    ])
    
    with tab1:
        st.subheader("Zone Utilization Analysis")
        
        # Get zone statistics
        zone_stats = zone_analytics.get_zone_statistics(start_time, end_time)
        
        if not zone_stats.empty:
            if selected_zone and selected_zone != 'All':
                zone_stats = zone_stats[zone_stats['zone_id'] == selected_zone]
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Utilization chart
                fig = px.bar(
                    zone_stats.sort_values('utilization', ascending=False).head(20),
                    x='zone_name',
                    y='utilization',
                    color='utilization',
                    title='Zone Utilization (%)',
                    color_continuous_scale='RdYlGn',
                    labels={'utilization': 'Utilization (%)', 'zone_name': 'Zone'}
                )
                fig.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Occupancy time chart
                fig = chart_builder.build_zone_occupancy_chart(zone_stats)
                st.plotly_chart(fig, use_container_width=True)
        
        # Hourly pattern
        st.subheader("Hourly Utilization Pattern")
        
        hourly_pattern = db_manager.query_dataframe("""
            SELECT 
                HOUR(ts) as hour,
                COUNT(DISTINCT agv_id) as avg_agvs,
                COUNT(*) / (COUNT(DISTINCT DATE(ts)) * 3 * 3600) * 100 as utilization
            FROM agv_positions
            WHERE ts BETWEEN %s AND %s
            {} 
            GROUP BY HOUR(ts)
            ORDER BY hour
        """.format(
            f"AND zone_id = '{selected_zone}'" if selected_zone and selected_zone != 'All' else ""
        ), (start_time, end_time))
        
        if not hourly_pattern.empty:
            fig = px.line(
                hourly_pattern,
                x='hour',
                y='avg_agvs',
                title='Average AGVs by Hour',
                labels={'hour': 'Hour of Day', 'avg_agvs': 'Average AGVs'},
                markers=True
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Zone Transition Analysis")
        
        # Get transition data
        transitions = zone_analytics.get_zone_transitions(start_time, end_time)
        
        if not transitions.empty:
            # Filter for selected zone if applicable
            if selected_zone and selected_zone != 'All':
                transitions = transitions[
                    (transitions['from_zone'] == selected_zone) |
                    (transitions['to_zone'] == selected_zone)
                ]
            
            # Transition matrix
            st.write("**Zone Transition Matrix**")
            fig = chart_builder.build_zone_transition_matrix(transitions)
            st.plotly_chart(fig, use_container_width=True)
            
            # Flow analysis
            col1, col2 = st.columns(2)
            
            with col1:
                # Top origins
                if selected_zone and selected_zone != 'All':
                    origins = transitions[transitions['to_zone'] == selected_zone].groupby('from_zone').agg({
                        'transition_count': 'sum',
                        'unique_agvs': 'sum'
                    }).sort_values('transition_count', ascending=False).head(10)
                    
                    if not origins.empty:
                        st.write(f"**Top Origins for {selected_zone}**")
                        st.dataframe(origins)
            
            with col2:
                # Top destinations
                if selected_zone and selected_zone != 'All':
                    destinations = transitions[transitions['from_zone'] == selected_zone].groupby('to_zone').agg({
                        'transition_count': 'sum',
                        'unique_agvs': 'sum'
                    }).sort_values('transition_count', ascending=False).head(10)
                    
                    if not destinations.empty:
                        st.write(f"**Top Destinations from {selected_zone}**")
                        st.dataframe(destinations)
        
        # Flow rates
        flow_rates = zone_analytics.calculate_zone_flow(end_time - start_time)
        
        if flow_rates:
            st.subheader("Zone Flow Rates")
            
            # Convert to DataFrame for display
            flow_df = pd.DataFrame([
                {
                    'From': v['from'],
                    'To': v['to'],
                    'Flow Rate (AGVs/hr)': v['flow_rate'],
                    'Unique AGVs': v['unique_agvs']
                }
                for v in flow_rates.values()
            ]).sort_values('Flow Rate (AGVs/hr)', ascending=False)
            
            if selected_zone and selected_zone != 'All':
                flow_df = flow_df[
                    (flow_df['From'] == selected_zone) |
                    (flow_df['To'] == selected_zone)
                ]
            
            st.dataframe(flow_df.head(20), use_container_width=True)
    
    with tab3:
        st.subheader("Bottleneck Detection")
        
        # Detect bottlenecks
        bottlenecks = zone_analytics.find_bottlenecks(end_time - start_time)
        
        if bottlenecks:
            # Filter for selected zone if applicable
            if selected_zone and selected_zone != 'All':
                bottlenecks = [b for b in bottlenecks if b['zone_id'] == selected_zone]
            
            if bottlenecks:
                # Create bottleneck map
                fig = map_renderer.create_base_figure("Bottleneck Locations")
                fig = map_renderer.add_zones(fig, highlight_zones=[b['zone_id'] for b in bottlenecks])
                
                # Add bottleneck indicators
                for bottleneck in bottlenecks:
                    zone_info = zone_analytics.zone_manager.get_zone_info(bottleneck['zone_id'])
                    if zone_info and zone_info.get('centroid_x'):
                        color = 'red' if bottleneck['severity'] == 'HIGH' else 'orange'
                        
                        fig.add_trace(go.Scatter(
                            x=[zone_info['centroid_x']],
                            y=[zone_info['centroid_y']],
                            mode='markers+text',
                            marker=dict(size=20, color=color, symbol='triangle-up'),
                            text=bottleneck['type'],
                            textposition='top center',
                            name=f"Bottleneck - {bottleneck['zone_name']}",
                            hovertext=f"{bottleneck['type']}: {bottleneck.get('utilization', 0):.1f}%"
                        ))
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Bottleneck details
                st.write("**Bottleneck Details**")
                
                for bottleneck in bottlenecks:
                    severity_color = {
                        'HIGH': 'red',
                        'MEDIUM': 'orange',
                        'LOW': 'yellow'
                    }.get(bottleneck['severity'], 'gray')
                    
                    with st.expander(f"{bottleneck['zone_name']} - {bottleneck['type']}"):
                        st.markdown(f"**Severity:** :{severity_color}[{bottleneck['severity']}]")
                        
                        if bottleneck['type'] == 'OCCUPANCY':
                            st.write(f"Current AGVs: {bottleneck['current_agvs']}")
                            st.write(f"Maximum AGVs: {bottleneck['max_agvs']}")
                            st.write(f"Utilization: {bottleneck['utilization']:.1f}%")
                            
                            st.write("**Recommendation:** Consider increasing zone capacity or rerouting AGVs")
                        
                        elif bottleneck['type'] == 'SPEED':
                            st.write(f"Average Speed: {bottleneck['avg_speed']:.2f} m/s")
                            st.write(f"Maximum Speed: {bottleneck['max_speed']:.2f} m/s")
                            st.write(f"Speed Ratio: {bottleneck['speed_ratio']:.1f}%")
                            
                            st.write("**Recommendation:** Investigate cause of speed reduction")
            else:
                st.info("No bottlenecks detected for selected zone")
        else:
            st.success("No bottlenecks detected")
    
    with tab4:
        st.subheader("Zone Optimization Recommendations")
        
        if show_recommendations:
            # Get optimization suggestions
            suggestions = zone_analytics.optimize_zone_allocation()
            
            if suggestions:
                # Filter for selected zone if applicable
                if selected_zone and selected_zone != 'All':
                    suggestions = [s for s in suggestions if s['zone_id'] == selected_zone]
                
                if suggestions:
                    for suggestion in suggestions:
                        with st.expander(f"{suggestion['zone_name']} - {suggestion['type']}"):
                            st.write(f"**Current Max AGVs:** {suggestion['current_max']}")
                            
                            if 'suggested_max' in suggestion:
                                st.write(f"**Suggested Max AGVs:** {suggestion['suggested_max']}")
                            
                            if 'suggested_strategy' in suggestion:
                                st.write(f"**Suggested Strategy:** {suggestion['suggested_strategy']}")
                            
                            st.write(f"**Reason:** {suggestion['reason']}")
                            
                            if 'potential_savings' in suggestion:
                                st.write(f"**Potential Benefit:** {suggestion['potential_savings']}")
                            elif 'potential_benefit' in suggestion:
                                st.write(f"**Potential Benefit:** {suggestion['potential_benefit']}")
                            
                            if suggestion['type'] == 'HIGH_VARIABILITY':
                                st.write(f"**Standard Deviation:** {suggestion['std_deviation']:.2f} AGVs")
                else:
                    st.info("No optimization suggestions for selected zone")
            else:
                st.info("No optimization suggestions available")
        
        # Zone comparison
        st.subheader("Zone Performance Comparison")
        
        zone_comparison = db_manager.query_dataframe("""
            SELECT 
                z.zone_id,
                z.name,
                z.category,
                z.max_agvs,
                COUNT(DISTINCT p.agv_id) as avg_agvs,
                COUNT(*) / (3.0 * 3600) as hours_occupied,
                AVG(p.speed_mps) as avg_speed,
                z.max_speed_mps
            FROM plant_zones z
            LEFT JOIN agv_positions p ON z.zone_id = p.zone_id
                AND p.ts BETWEEN %s AND %s
            WHERE z.active = TRUE
            GROUP BY z.zone_id, z.name, z.category, z.max_agvs, z.max_speed_mps
        """, (start_time, end_time))
        
        if not zone_comparison.empty:
            # Calculate efficiency metrics
            zone_comparison['occupancy_ratio'] = zone_comparison['avg_agvs'] / zone_comparison['max_agvs']
            zone_comparison['speed_ratio'] = zone_comparison['avg_speed'] / zone_comparison['max_speed_mps']
            
            # Scatter plot
            fig = px.scatter(
                zone_comparison,
                x='occupancy_ratio',
                y='speed_ratio',
                size='hours_occupied',
                color='category',
                hover_data=['name', 'avg_agvs', 'avg_speed'],
                title='Zone Efficiency Matrix',
                labels={
                    'occupancy_ratio': 'Occupancy Ratio',
                    'speed_ratio': 'Speed Efficiency',
                    'hours_occupied': 'Hours Occupied'
                }
            )
            
            # Add quadrant lines
            fig.add_hline(y=0.5, line_dash="dash", line_color="gray", opacity=0.5)
            fig.add_vline(x=0.5, line_dash="dash", line_color="gray", opacity=0.5)
            
            # Add quadrant labels
            fig.add_annotation(x=0.25, y=0.75, text="Low Use<br>High Speed", showarrow=False)
            fig.add_annotation(x=0.75, y=0.75, text="High Use<br>High Speed", showarrow=False)
            fig.add_annotation(x=0.25, y=0.25, text="Low Use<br>Low Speed", showarrow=False)
            fig.add_annotation(x=0.75, y=0.25, text="High Use<br>Low Speed", showarrow=False)
            
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab5:
        st.subheader("Zone Demand Predictions")
        
        if show_predictions:
            if selected_zone and selected_zone != 'All':
                # Get predictions for selected zone
                predictions = zone_analytics.predict_zone_demand(selected_zone, forecast_hours=6)
                
                if not predictions.empty:
                    # Prediction chart
                    fig = px.line(
                        predictions,
                        x='forecast_time',
                        y='predicted_agvs',
                        title=f'Predicted AGV Demand - {selected_zone}',
                        labels={'predicted_agvs': 'Predicted AGVs', 'forecast_time': 'Time'},
                        markers=True
                    )
                    
                    # Add confidence bands
                    fig.add_scatter(
                        x=predictions['forecast_time'],
                        y=predictions['predicted_agvs'] * predictions['confidence'],
                        mode='lines',
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo='skip'
                    )
                    fig.add_scatter(
                        x=predictions['forecast_time'],
                        y=predictions['predicted_agvs'] / predictions['confidence'],
                        mode='lines',
                        line=dict(width=0),
                        fill='tonexty',
                        fillcolor='rgba(0,100,200,0.2)',
                        showlegend=False,
                        hoverinfo='skip'
                    )
                    
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Prediction table
                    st.write("**Detailed Predictions**")
                    st.dataframe(predictions, use_container_width=True)
                else:
                    st.info("Insufficient historical data for predictions")
            else:
                st.info("Please select a specific zone for predictions")
        else:
            st.info("Enable predictions in the sidebar to see demand forecasts")
    
    # Auto-refresh
    if refresh_options['auto_refresh']:
        import time
        time.sleep(refresh_options['refresh_rate'])
        st.rerun()


if __name__ == "__main__":
    render_page()