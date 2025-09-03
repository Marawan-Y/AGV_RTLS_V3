"""
Chart builders for dashboard visualizations.
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from loguru import logger


class ChartBuilder:
    """Builds various chart types for the dashboard."""
    
    def __init__(self, theme: str = 'plotly_dark'):
        self.theme = theme
        self.colors = self._get_color_palette()
    
    def _get_color_palette(self) -> Dict[str, str]:
        """Get color palette for charts."""
        return {
            'primary': '#1f77b4',
            'secondary': '#ff7f0e',
            'success': '#2ca02c',
            'warning': '#ff9800',
            'danger': '#d62728',
            'info': '#17a2b8',
            'light': '#f8f9fa',
            'dark': '#343a40',
            'trajectory': '#0066cc',
            'zones': {
                'RAW_MATERIAL': '#2E7D32',
                'PRODUCTION': '#1565C0',
                'ASSEMBLY': '#6A1B9A',
                'STAGING': '#F57C00',
                'MAINTENANCE': '#C62828',
                'RESTRICTED': '#B71C1C',
                'TRANSIT': '#546E7A',
                'LOGISTICS': '#00838F'
            }
        }
    
    def build_trajectory_chart(self, trajectory_df: pd.DataFrame,
                              plant_map: Optional[Any] = None,
                              show_arrows: bool = False) -> go.Figure:
        """Build trajectory visualization chart."""
        
        fig = go.Figure()
        
        # Add plant map background if provided
        if plant_map:
            fig.add_layout_image(
                dict(
                    source=plant_map,
                    xref="x",
                    yref="y",
                    x=0,
                    y=150,  # Assuming plant height
                    sizex=200,  # Assuming plant width
                    sizey=150,
                    sizing="stretch",
                    opacity=0.5,
                    layer="below"
                )
            )
        
        # Add trajectory line
        if not trajectory_df.empty:
            fig.add_trace(go.Scatter(
                x=trajectory_df['plant_x'],
                y=trajectory_df['plant_y'],
                mode='lines+markers',
                name='Trajectory',
                line=dict(
                    color=self.colors['trajectory'],
                    width=3
                ),
                marker=dict(
                    size=5,
                    color=trajectory_df.get('speed_mps', 0),
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Speed (m/s)")
                ),
                hovertemplate=(
                    'X: %{x:.1f}<br>'
                    'Y: %{y:.1f}<br>'
                    'Speed: %{marker.color:.1f} m/s<br>'
                    '<extra></extra>'
                )
            ))
            
            # Add direction arrows
            if show_arrows and len(trajectory_df) > 10:
                arrow_indices = np.linspace(0, len(trajectory_df)-1, 15, dtype=int)
                
                for idx in arrow_indices[:-1]:  # Skip last point
                    row = trajectory_df.iloc[idx]
                    next_row = trajectory_df.iloc[min(idx+1, len(trajectory_df)-1)]
                    
                    fig.add_annotation(
                        x=row['plant_x'],
                        y=row['plant_y'],
                        ax=next_row['plant_x'],
                        ay=next_row['plant_y'],
                        xref="x",
                        yref="y",
                        axref="x",
                        ayref="y",
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=2,
                        arrowcolor="rgba(0,100,200,0.5)"
                    )
            
            # Add start and end markers
            fig.add_trace(go.Scatter(
                x=[trajectory_df.iloc[0]['plant_x']],
                y=[trajectory_df.iloc[0]['plant_y']],
                mode='markers',
                name='Start',
                marker=dict(size=15, color='green', symbol='circle')
            ))
            
            fig.add_trace(go.Scatter(
                x=[trajectory_df.iloc[-1]['plant_x']],
                y=[trajectory_df.iloc[-1]['plant_y']],
                mode='markers',
                name='End',
                marker=dict(size=15, color='red', symbol='square')
            ))
        
        # Update layout
        fig.update_layout(
            title="AGV Trajectory",
            xaxis=dict(
                title="X Position (m)",
                scaleanchor="y",
                scaleratio=1
            ),
            yaxis=dict(
                title="Y Position (m)"
            ),
            hovermode='closest',
            template=self.theme,
            height=600
        )
        
        return fig
    
    def build_speed_profile(self, trajectory_df: pd.DataFrame) -> go.Figure:
        """Build speed profile chart."""
        
        fig = go.Figure()
        
        if not trajectory_df.empty and 'speed_mps' in trajectory_df.columns:
            fig.add_trace(go.Scatter(
                x=trajectory_df.index if 'ts' not in trajectory_df.columns else trajectory_df['ts'],
                y=trajectory_df['speed_mps'],
                mode='lines',
                name='Speed',
                line=dict(color=self.colors['primary'], width=2),
                fill='tozeroy',
                fillcolor='rgba(31,119,180,0.2)'
            ))
            
            # Add average line
            avg_speed = trajectory_df['speed_mps'].mean()
            fig.add_hline(
                y=avg_speed,
                line_dash="dash",
                line_color=self.colors['secondary'],
                annotation_text=f"Avg: {avg_speed:.2f} m/s"
            )
        
        fig.update_layout(
            title="Speed Profile",
            xaxis_title="Time",
            yaxis_title="Speed (m/s)",
            template=self.theme,
            height=300
        )
        
        return fig
    
    def build_zone_occupancy_chart(self, zone_data: pd.DataFrame) -> go.Figure:
        """Build zone occupancy bar chart."""
        
        if zone_data.empty:
            return go.Figure()
        
        # Sort by occupancy
        zone_data = zone_data.sort_values('occupancy_time_min', ascending=True)
        
        # Create horizontal bar chart
        fig = go.Figure(go.Bar(
            x=zone_data['occupancy_time_min'],
            y=zone_data['zone_name'],
            orientation='h',
            marker=dict(
                color=[self.colors['zones'].get(cat, '#808080') 
                      for cat in zone_data['category']],
                line=dict(color='rgba(0,0,0,0.3)', width=1)
            ),
            text=zone_data['occupancy_time_min'].round(1),
            textposition='outside',
            hovertemplate=(
                'Zone: %{y}<br>'
                'Time: %{x:.1f} minutes<br>'
                'AGVs: %{customdata}<br>'
                '<extra></extra>'
            ),
            customdata=zone_data['unique_agvs']
        ))
        
        fig.update_layout(
            title="Zone Occupancy (Last 24h)",
            xaxis_title="Time (minutes)",
            yaxis_title="Zone",
            template=self.theme,
            height=400,
            margin=dict(l=150)
        )
        
        return fig
    
    def build_fleet_utilization_gauge(self, utilization: float) -> go.Figure:
        """Build fleet utilization gauge chart."""
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=utilization,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Fleet Utilization"},
            delta={'reference': 85, 'suffix': '%'},
            gauge={
                'axis': {'range': [None, 100], 'ticksuffix': '%'},
                'bar': {'color': self._get_gauge_color(utilization)},
                'steps': [
                    {'range': [0, 50], 'color': "lightgray"},
                    {'range': [50, 85], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        
        fig.update_layout(
            height=250,
            template=self.theme
        )
        
        return fig
    
    def _get_gauge_color(self, value: float) -> str:
        """Get color for gauge based on value."""
        if value >= 85:
            return self.colors['success']
        elif value >= 70:
            return self.colors['warning']
        else:
            return self.colors['danger']
    
    def build_heatmap(self, heatmap_data: Dict) -> go.Figure:
        """Build position density heatmap."""
        
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data['z'],
            x=heatmap_data['x'],
            y=heatmap_data['y'],
            colorscale='Jet',
            showscale=True,
            colorbar=dict(
                title="Density",
                thickness=15,
                len=0.7
            ),
            hovertemplate='X: %{x:.1f}<br>Y: %{y:.1f}<br>Density: %{z:.2f}<extra></extra>'
        ))
        
        fig.update_layout(
            title="Position Density Heatmap",
            xaxis=dict(
                title="X Position (m)",
                scaleanchor="y",
                scaleratio=1
            ),
            yaxis=dict(
                title="Y Position (m)"
            ),
            template=self.theme,
            height=600
        )
        
        return fig
    
    def build_kpi_cards(self, kpis: Dict) -> List[go.Figure]:
        """Build KPI indicator cards."""
        
        cards = []
        
        kpi_configs = [
            ('efficiency', 'Efficiency', '%', 'gauge'),
            ('availability', 'Availability', '%', 'gauge'),
            ('throughput', 'Throughput', 'tasks/hr', 'number'),
            ('avg_task_time', 'Avg Task Time', 'min', 'number')
        ]
        
        for key, title, suffix, mode in kpi_configs:
            if key in kpis:
                value = kpis[key]
                change = kpis.get(f'{key}_change', 0)
                
                if mode == 'gauge':
                    fig = self.build_fleet_utilization_gauge(value)
                    fig.update_traces(title={'text': title})
                else:
                    fig = go.Figure(go.Indicator(
                        mode="number+delta",
                        value=value,
                        title={'text': title},
                        delta={'reference': value - change, 'suffix': suffix},
                        number={'suffix': f" {suffix}"}
                    ))
                    
                    fig.update_layout(
                        height=150,
                        template=self.theme
                    )
                
                cards.append(fig)
        
        return cards
    
    def build_anomaly_timeline(self, anomalies: pd.DataFrame) -> go.Figure:
        """Build anomaly timeline chart."""
        
        if anomalies.empty:
            return go.Figure()
        
        # Group by time and severity
        anomalies['created_at'] = pd.to_datetime(anomalies['created_at'])
        hourly = anomalies.set_index('created_at').resample('1H')['severity'].value_counts().unstack(fill_value=0)
        
        fig = go.Figure()
        
        severity_colors = {
            'CRITICAL': self.colors['danger'],
            'ERROR': '#ff6600',
            'WARNING': self.colors['warning'],
            'INFO': self.colors['info']
        }
        
        for severity in ['CRITICAL', 'ERROR', 'WARNING', 'INFO']:
            if severity in hourly.columns:
                fig.add_trace(go.Scatter(
                    x=hourly.index,
                    y=hourly[severity],
                    mode='lines',
                    name=severity,
                    line=dict(color=severity_colors[severity], width=2),
                    stackgroup='one'
                ))
        
        fig.update_layout(
            title="Anomaly Timeline",
            xaxis_title="Time",
            yaxis_title="Count",
            template=self.theme,
            height=300,
            hovermode='x unified'
        )
        
        return fig
    
    def build_zone_transition_matrix(self, transitions: pd.DataFrame) -> go.Figure:
        """Build zone transition matrix heatmap."""
        
        if transitions.empty:
            return go.Figure()
        
        # Create pivot table
        matrix = transitions.pivot_table(
            index='from_zone',
            columns='to_zone',
            values='transition_count',
            fill_value=0
        )
        
        fig = go.Figure(data=go.Heatmap(
            z=matrix.values,
            x=matrix.columns,
            y=matrix.index,
            colorscale='Blues',
            text=matrix.values,
            texttemplate='%{text}',
            textfont={"size": 10},
            hovertemplate='From: %{y}<br>To: %{x}<br>Count: %{z}<extra></extra>'
        ))
        
        fig.update_layout(
            title="Zone Transition Matrix",
            xaxis=dict(title="To Zone", side="bottom"),
            yaxis=dict(title="From Zone", autorange='reversed'),
            template=self.theme,
            height=500
        )
        
        return fig
    
    def build_battery_status_chart(self, fleet_status: pd.DataFrame) -> go.Figure:
        """Build battery status chart for fleet."""
        
        if fleet_status.empty or 'battery_percent' not in fleet_status.columns:
            return go.Figure()
        
        # Categorize battery levels
        fleet_status['battery_category'] = pd.cut(
            fleet_status['battery_percent'],
            bins=[0, 20, 50, 80, 100],
            labels=['Critical', 'Low', 'Medium', 'Good']
        )
        
        counts = fleet_status['battery_category'].value_counts()
        
        colors_map = {
            'Critical': self.colors['danger'],
            'Low': self.colors['warning'],
            'Medium': self.colors['info'],
            'Good': self.colors['success']
        }
        
        fig = go.Figure(data=[go.Pie(
            labels=counts.index,
            values=counts.values,
            marker=dict(colors=[colors_map[cat] for cat in counts.index]),
            hole=0.3,
            textinfo='label+percent',
            textposition='outside'
        )])
        
        fig.update_layout(
            title="Fleet Battery Status",
            template=self.theme,
            height=300
        )
        
        return fig
    
    def build_task_timeline(self, tasks: pd.DataFrame) -> go.Figure:
        """Build Gantt chart for tasks."""
        
        if tasks.empty:
            return go.Figure()
        
        # Prepare data for Gantt chart
        tasks['started_at'] = pd.to_datetime(tasks['started_at'])
        tasks['completed_at'] = pd.to_datetime(tasks.get('completed_at', tasks['started_at'] + pd.Timedelta(minutes=15)))
        
        fig = px.timeline(
            tasks,
            x_start="started_at",
            x_end="completed_at",
            y="agv_id",
            color="status",
            title="Task Timeline",
            labels={"agv_id": "AGV"},
            hover_data=["task_type", "origin_zone_id", "destination_zone_id"]
        )
        
        fig.update_layout(
            template=self.theme,
            height=400
        )
        
        return fig