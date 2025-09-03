"""
Filter components for dashboard.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date, time
from typing import Dict, List, Optional, Tuple

from src.core.database import db_manager


class FilterComponents:
    """Reusable filter components for dashboard pages."""
    
    def __init__(self):
        self.agv_list = self._load_agv_list()
        self.zone_list = self._load_zone_list()
    
    def _load_agv_list(self) -> List[str]:
        """Load list of AGVs from database."""
        try:
            result = db_manager.execute_query(
                "SELECT DISTINCT agv_id FROM agv_registry ORDER BY agv_id"
            )
            return [r['agv_id'] for r in result]
        except:
            return []
    
    def _load_zone_list(self) -> List[str]:
        """Load list of zones from database."""
        try:
            result = db_manager.execute_query(
                "SELECT zone_id, name FROM plant_zones WHERE active = TRUE ORDER BY zone_id"
            )
            return [(r['zone_id'], r['name']) for r in result]
        except:
            return []
    
    def render_agv_selector(self, key: str = "agv_select", 
                           multi: bool = False,
                           include_all: bool = True) -> Optional[List[str]]:
        """Render AGV selector."""
        
        options = self.agv_list.copy()
        
        if include_all and not multi:
            options = ['All'] + options
        
        if multi:
            selected = st.multiselect(
                "Select AGVs",
                options=options,
                default=[],
                key=key
            )
        else:
            selected = st.selectbox(
                "Select AGV",
                options=options,
                index=0,
                key=key
            )
        
        return selected
    
    def render_zone_selector(self, key: str = "zone_select",
                           multi: bool = False,
                           include_all: bool = True) -> Optional[List[str]]:
        """Render zone selector."""
        
        zone_options = {f"{z[0]} - {z[1]}": z[0] for z in self.zone_list}
        
        if include_all and not multi:
            zone_options = {'All Zones': None, **zone_options}
        
        if multi:
            selected_names = st.multiselect(
                "Select Zones",
                options=list(zone_options.keys()),
                default=[],
                key=key
            )
            return [zone_options[name] for name in selected_names]
        else:
            selected_name = st.selectbox(
                "Select Zone",
                options=list(zone_options.keys()),
                index=0,
                key=key
            )
            return zone_options[selected_name]
    
    def render_time_range_selector(self, key_prefix: str = "time",
                                  default_hours: int = 24) -> Tuple[datetime, datetime]:
        """Render time range selector."""
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            preset = st.selectbox(
                "Quick Select",
                options=[
                    "Last Hour",
                    "Last 6 Hours",
                    "Last 24 Hours",
                    "Last 7 Days",
                    "Custom"
                ],
                index=2,
                key=f"{key_prefix}_preset"
            )
        
        if preset == "Custom":
            with col2:
                start_date = st.date_input(
                    "Start Date",
                    value=date.today() - timedelta(days=1),
                    key=f"{key_prefix}_start_date"
                )
                start_time = st.time_input(
                    "Start Time",
                    value=time(0, 0),
                    key=f"{key_prefix}_start_time"
                )
            
            with col3:
                end_date = st.date_input(
                    "End Date",
                    value=date.today(),
                    key=f"{key_prefix}_end_date"
                )
                end_time = st.time_input(
                    "End Time",
                    value=datetime.now().time(),
                    key=f"{key_prefix}_end_time"
                )
            
            start_datetime = datetime.combine(start_date, start_time)
            end_datetime = datetime.combine(end_date, end_time)
        else:
            end_datetime = datetime.now()
            
            if preset == "Last Hour":
                start_datetime = end_datetime - timedelta(hours=1)
            elif preset == "Last 6 Hours":
                start_datetime = end_datetime - timedelta(hours=6)
            elif preset == "Last 24 Hours":
                start_datetime = end_datetime - timedelta(hours=24)
            elif preset == "Last 7 Days":
                start_datetime = end_datetime - timedelta(days=7)
            else:
                start_datetime = end_datetime - timedelta(hours=default_hours)
        
        return start_datetime, end_datetime
    
    def render_display_options(self, key_prefix: str = "display") -> Dict:
        """Render display options."""
        
        with st.expander("Display Options", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                show_trajectory = st.checkbox(
                    "Show Trajectory",
                    value=True,
                    key=f"{key_prefix}_trajectory"
                )
                show_heatmap = st.checkbox(
                    "Show Heatmap",
                    value=False,
                    key=f"{key_prefix}_heatmap"
                )
                show_zones = st.checkbox(
                    "Show Zones",
                    value=True,
                    key=f"{key_prefix}_zones"
                )
            
            with col2:
                show_arrows = st.checkbox(
                    "Show Direction Arrows",
                    value=False,
                    key=f"{key_prefix}_arrows"
                )
                show_stops = st.checkbox(
                    "Show Stop Points",
                    value=False,
                    key=f"{key_prefix}_stops"
                )
                animation_speed = st.slider(
                    "Animation Speed",
                    min_value=0.5,
                    max_value=5.0,
                    value=1.0,
                    step=0.5,
                    key=f"{key_prefix}_speed"
                )
        
        return {
            'show_trajectory': show_trajectory,
            'show_heatmap': show_heatmap,
            'show_zones': show_zones,
            'show_arrows': show_arrows,
            'show_stops': show_stops,
            'animation_speed': animation_speed
        }
    
    def render_refresh_controls(self, key_prefix: str = "refresh") -> Dict:
        """Render refresh controls."""
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            auto_refresh = st.checkbox(
                "Auto Refresh",
                value=False,
                key=f"{key_prefix}_auto"
            )
        
        with col2:
            if auto_refresh:
                refresh_rate = st.slider(
                    "Refresh Rate (seconds)",
                    min_value=1,
                    max_value=30,
                    value=5,
                    key=f"{key_prefix}_rate"
                )
            else:
                refresh_rate = None
                if st.button("Refresh Now", key=f"{key_prefix}_manual"):
                    st.rerun()
        
        return {
            'auto_refresh': auto_refresh,
            'refresh_rate': refresh_rate
        }
    
    def render_task_filters(self, key_prefix: str = "task") -> Dict:
        """Render task-specific filters."""
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status = st.selectbox(
                "Task Status",
                options=['All', 'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED'],
                index=0,
                key=f"{key_prefix}_status"
            )
        
        with col2:
            priority = st.selectbox(
                "Priority",
                options=['All', 'High (8-10)', 'Medium (4-7)', 'Low (1-3)'],
                index=0,
                key=f"{key_prefix}_priority"
            )
        
        with col3:
            task_type = st.selectbox(
                "Task Type",
                options=['All', 'TRANSPORT', 'PICKUP', 'DELIVERY', 'CHARGING'],
                index=0,
                key=f"{key_prefix}_type"
            )
        
        return {
            'status': None if status == 'All' else status,
            'priority': priority,
            'task_type': None if task_type == 'All' else task_type
        }
    
    def render_alert_filters(self, key_prefix: str = "alert") -> Dict:
        """Render alert/event filters."""
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            severity = st.multiselect(
                "Severity",
                options=['CRITICAL', 'ERROR', 'WARNING', 'INFO'],
                default=['CRITICAL', 'ERROR'],
                key=f"{key_prefix}_severity"
            )
        
        with col2:
            event_types = st.multiselect(
                "Event Types",
                options=[
                    'ANOMALY_DETECTED',
                    'ZONE_VIOLATION',
                    'SPEED_VIOLATION',
                    'COLLISION_RISK',
                    'BATTERY_LOW',
                    'CONNECTION_LOST'
                ],
                default=[],
                key=f"{key_prefix}_types"
            )
        
        with col3:
            acknowledged = st.selectbox(
                "Acknowledgment Status",
                options=['All', 'Acknowledged', 'Unacknowledged'],
                index=2,
                key=f"{key_prefix}_ack"
            )
        
        return {
            'severity': severity,
            'event_types': event_types if event_types else None,
            'acknowledged': None if acknowledged == 'All' else (acknowledged == 'Acknowledged')
        }