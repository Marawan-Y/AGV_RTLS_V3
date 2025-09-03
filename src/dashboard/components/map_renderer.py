"""
Map rendering component for dashboard.
"""

import plotly.graph_objects as go
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from PIL import Image
import json

from loguru import logger
from src.core.zone_manager import ZoneManager


class MapRenderer:
    """Renders plant map with AGV positions and zones."""
    
    def __init__(self, plant_map_path: str = 'assets/plant_map.png'):
        self.plant_map = self._load_plant_map(plant_map_path)
        self.zone_manager = ZoneManager()
        self.plant_bounds = self._get_plant_bounds()
    
    def _load_plant_map(self, path: str) -> Optional[Image.Image]:
        """Load plant map image."""
        try:
            return Image.open(path)
        except Exception as e:
            logger.warning(f"Could not load plant map: {e}")
            return None
    
    def _get_plant_bounds(self) -> Dict[str, float]:
        """Get plant coordinate bounds."""
        import os
        return {
            'xmin': float(os.getenv('PLANT_XMIN', 0)),
            'xmax': float(os.getenv('PLANT_XMAX', 200)),
            'ymin': float(os.getenv('PLANT_YMIN', 0)),
            'ymax': float(os.getenv('PLANT_YMAX', 150))
        }
    
    def create_base_figure(self, title: str = "Plant Map") -> go.Figure:
        """Create base figure with plant map background."""
        
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
                    opacity=0.6,
                    layer="below"
                )
            )
        
        # Set layout
        fig.update_layout(
            title=title,
            xaxis=dict(
                title="X Position (m)",
                range=[self.plant_bounds['xmin'], self.plant_bounds['xmax']],
                scaleanchor="y",
                scaleratio=1,
                showgrid=True,
                gridwidth=1,
                gridcolor='rgba(128,128,128,0.2)'
            ),
            yaxis=dict(
                title="Y Position (m)",
                range=[self.plant_bounds['ymin'], self.plant_bounds['ymax']],
                showgrid=True,
                gridwidth=1,
                gridcolor='rgba(128,128,128,0.2)'
            ),
            hovermode='closest',
            height=700,
            template='plotly_white'
        )
        
        return fig
    
    def add_zones(self, fig: go.Figure, show_labels: bool = True,
                 highlight_zones: Optional[List[str]] = None) -> go.Figure:
        """Add zone boundaries to figure."""
        
        zones = self.zone_manager.get_all_zones()
        
        zone_colors = {
            'RAW_MATERIAL': 'rgba(46,125,50,0.2)',
            'PRODUCTION': 'rgba(21,101,192,0.2)',
            'ASSEMBLY': 'rgba(106,27,154,0.2)',
            'STAGING': 'rgba(245,124,0,0.2)',
            'MAINTENANCE': 'rgba(198,40,40,0.2)',
            'RESTRICTED': 'rgba(183,28,28,0.3)',
            'TRANSIT': 'rgba(84,110,122,0.2)',
            'LOGISTICS': 'rgba(0,131,143,0.2)'
        }
        
        for zone_id, zone_info in zones.items():
            if 'vertices' in zone_info and zone_info['vertices']:
                try:
                    vertices = json.loads(zone_info['vertices'])
                    vertices.append(vertices[0])  # Close the polygon
                    
                    x_coords = [v[0] for v in vertices]
                    y_coords = [v[1] for v in vertices]
                    
                    # Determine if zone should be highlighted
                    is_highlighted = highlight_zones and zone_id in highlight_zones
                    opacity = 0.4 if is_highlighted else 0.2
                    line_width = 3 if is_highlighted else 1
                    
                    # Add zone boundary
                    fig.add_trace(go.Scatter(
                        x=x_coords,
                        y=y_coords,
                        mode='lines',
                        name=f"Zone: {zone_info['name']}",
                        line=dict(
                            color='rgba(100,100,100,0.5)',
                            width=line_width
                        ),
                        fill='toself',
                        fillcolor=zone_colors.get(zone_info.get('category', 'TRANSIT')),
                        hoverinfo='text',
                        text=f"Zone: {zone_info['name']}<br>Type: {zone_info.get('category', 'Unknown')}",
                        showlegend=False
                    ))
                    
                    # Add zone label
                    if show_labels and zone_info.get('centroid_x'):
                        fig.add_annotation(
                            x=zone_info['centroid_x'],
                            y=zone_info['centroid_y'],
                            text=zone_info['name'],
                            showarrow=False,
                            font=dict(size=10, color='black'),
                            bgcolor='rgba(255,255,255,0.7)',
                            bordercolor='rgba(0,0,0,0.3)',
                            borderwidth=1
                        )
                        
                except Exception as e:
                    logger.error(f"Error rendering zone {zone_id}: {e}")
        
        return fig
    
    def add_agv_positions(self, fig: go.Figure, positions: pd.DataFrame,
                         show_labels: bool = True,
                         show_heading: bool = True) -> go.Figure:
        """Add AGV positions to figure."""
        
        if positions.empty:
            return fig
        
        # AGV type colors
        type_colors = {
            'TUGGER': '#1f77b4',
            'FORKLIFT': '#ff7f0e',
            'PALLET_JACK': '#2ca02c',
            'AMR': '#d62728',
            'AGC': '#9467bd'
        }
        
        # Group by AGV type for better visualization
        for agv_type in positions['type'].unique() if 'type' in positions.columns else ['Unknown']:
            type_positions = positions[positions['type'] == agv_type] if 'type' in positions.columns else positions
            
            # Add position markers
            fig.add_trace(go.Scatter(
                x=type_positions['plant_x'],
                y=type_positions['plant_y'],
                mode='markers+text' if show_labels else 'markers',
                name=agv_type,
                marker=dict(
                    size=15,
                    color=type_colors.get(agv_type, '#808080'),
                    symbol='arrow' if show_heading else 'circle',
                    angle=type_positions['heading_deg'] if show_heading and 'heading_deg' in type_positions.columns else 0,
                    line=dict(color='white', width=2)
                ),
                text=type_positions['agv_id'] if show_labels and 'agv_id' in type_positions.columns else None,
                textposition='top center',
                hovertemplate=(
                    'AGV: %{text}<br>'
                    'Position: (%{x:.1f}, %{y:.1f})<br>'
                    'Speed: %{customdata[0]:.1f} m/s<br>'
                    'Battery: %{customdata[1]:.0f}%<br>'
                    '<extra></extra>'
                ),
                customdata=type_positions[['speed_mps', 'battery_percent']].values if all(col in type_positions.columns for col in ['speed_mps', 'battery_percent']) else None
            ))
        
        return fig
    
    def add_trajectory(self, fig: go.Figure, trajectory: pd.DataFrame,
                      agv_id: str = None, color: str = '#0066cc',
                      show_arrows: bool = False) -> go.Figure:
        """Add AGV trajectory to figure."""
        
        if trajectory.empty:
            return fig
        
        # Main trajectory line
        fig.add_trace(go.Scatter(
            x=trajectory['plant_x'],
            y=trajectory['plant_y'],
            mode='lines',
            name=f"Trajectory{f' - {agv_id}' if agv_id else ''}",
            line=dict(color=color, width=3),
            hovertemplate=(
                'Time: %{text}<br>'
                'Position: (%{x:.1f}, %{y:.1f})<br>'
                'Speed: %{customdata:.1f} m/s<br>'
                '<extra></extra>'
            ),
            text=trajectory['ts'].dt.strftime('%H:%M:%S') if 'ts' in trajectory.columns else None,
            customdata=trajectory['speed_mps'] if 'speed_mps' in trajectory.columns else None
        ))
        
        # Add direction arrows
        if show_arrows and len(trajectory) > 10:
            arrow_indices = np.linspace(0, len(trajectory)-1, 15, dtype=int)
            
            for idx in arrow_indices[:-1]:
                row = trajectory.iloc[idx]
                next_row = trajectory.iloc[min(idx+1, len(trajectory)-1)]
                
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
                    arrowcolor=f"rgba{color[1:] if color.startswith('#') else color}",
                    opacity=0.5
                )
        
        # Add start and end markers
        fig.add_trace(go.Scatter(
            x=[trajectory.iloc[0]['plant_x'], trajectory.iloc[-1]['plant_x']],
            y=[trajectory.iloc[0]['plant_y'], trajectory.iloc[-1]['plant_y']],
            mode='markers',
            name='Start/End',
            marker=dict(
                size=[15, 15],
                color=['green', 'red'],
                symbol=['circle', 'square']
            ),
            showlegend=False
        ))
        
        return fig
    
    def add_heatmap_overlay(self, fig: go.Figure, heatmap_data: Dict,
                           opacity: float = 0.5) -> go.Figure:
        """Add heatmap overlay to figure."""
        
        if not heatmap_data or 'z' not in heatmap_data:
            return fig
        
        fig.add_trace(go.Heatmap(
            z=heatmap_data['z'],
            x=heatmap_data.get('x'),
            y=heatmap_data.get('y'),
            colorscale='Jet',
            opacity=opacity,
            showscale=True,
            colorbar=dict(
                title="Density",
                x=1.02,
                thickness=15
            ),
            hoverinfo='skip'
        ))
        
        return fig
    
    def add_alerts(self, fig: go.Figure, alerts: List[Dict]) -> go.Figure:
        """Add alert indicators to map."""
        
        if not alerts:
            return fig
        
        alert_colors = {
            'CRITICAL': 'red',
            'ERROR': 'orange',
            'WARNING': 'yellow',
            'INFO': 'lightblue'
        }
        
        for alert in alerts:
            if 'position' in alert and alert['position']:
                x, y = alert['position']
                
                fig.add_trace(go.Scatter(
                    x=[x],
                    y=[y],
                    mode='markers',
                    name='Alert',
                    marker=dict(
                        size=20,
                        color=alert_colors.get(alert.get('severity', 'INFO')),
                        symbol='triangle-up',
                        line=dict(color='black', width=2)
                    ),
                    hovertext=f"{alert.get('type', 'Alert')}<br>{alert.get('message', '')}",
                    hoverinfo='text',
                    showlegend=False
                ))
        
        return fig
    
    def create_animated_map(self, positions_over_time: List[pd.DataFrame],
                          timestamps: List[datetime]) -> go.Figure:
        """Create animated map showing AGV movements over time."""
        
        # Create frames for animation
        frames = []
        
        for i, (positions, timestamp) in enumerate(zip(positions_over_time, timestamps)):
            frame_data = []
            
            # Add AGV positions for this timestamp
            if not positions.empty:
                frame_data.append(go.Scatter(
                    x=positions['plant_x'],
                    y=positions['plant_y'],
                    mode='markers',
                    marker=dict(size=15, color='blue'),
                    text=positions['agv_id'] if 'agv_id' in positions.columns else None,
                    name='AGVs'
                ))
            
            frames.append(go.Frame(
                data=frame_data,
                name=str(i),
                traces=[0]
            ))
        
        # Create figure with first frame
        fig = self.create_base_figure("AGV Movement Animation")
        
        if positions_over_time and not positions_over_time[0].empty:
            fig.add_trace(go.Scatter(
                x=positions_over_time[0]['plant_x'],
                y=positions_over_time[0]['plant_y'],
                mode='markers',
                marker=dict(size=15, color='blue'),
                text=positions_over_time[0]['agv_id'] if 'agv_id' in positions_over_time[0].columns else None,
                name='AGVs'
            ))
        
        # Add animation controls
        fig.update_layout(
            updatemenus=[{
                'type': 'buttons',
                'showactive': False,
                'buttons': [
                    {
                        'label': 'Play',
                        'method': 'animate',
                        'args': [None, {
                            'frame': {'duration': 300, 'redraw': True},
                            'fromcurrent': True,
                            'transition': {'duration': 0}
                        }]
                    },
                    {
                        'label': 'Pause',
                        'method': 'animate',
                        'args': [[None], {
                            'frame': {'duration': 0, 'redraw': False},
                            'mode': 'immediate',
                            'transition': {'duration': 0}
                        }]
                    }
                ]
            }],
            sliders=[{
                'steps': [
                    {
                        'args': [[f.name], {
                            'frame': {'duration': 300, 'redraw': True},
                            'mode': 'immediate',
                            'transition': {'duration': 0}
                        }],
                        'label': timestamps[i].strftime('%H:%M:%S'),
                        'method': 'animate'
                    }
                    for i, f in enumerate(frames)
                ],
                'active': 0,
                'y': 0,
                'len': 0.9,
                'x': 0.1,
                'xanchor': 'left',
                'y': 0,
                'yanchor': 'top'
            }]
        )
        
        fig.frames = frames
        
        return fig