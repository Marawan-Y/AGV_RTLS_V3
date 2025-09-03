"""
Zone-based analytics for AGV operations.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import networkx as nx
from collections import defaultdict

from loguru import logger
from src.core.database import db_manager


class ZoneAnalytics:
    """Analyzes zone utilization and transitions."""
    
    def __init__(self):
        self.zones = self._load_zones()
        self.transition_graph = nx.DiGraph()
        
    def _load_zones(self) -> pd.DataFrame:
        """Load zone definitions from database."""
        return db_manager.query_dataframe("""
            SELECT 
                zone_id, name, category, zone_type,
                max_speed_mps, max_agvs, priority,
                centroid_x, centroid_y, area_sqm
            FROM plant_zones
            WHERE active = TRUE
        """)
    
    def get_zones(self) -> pd.DataFrame:
        """Get zone information."""
        return self.zones
    
    def get_zone_statistics(self, start_time: datetime, 
                           end_time: datetime) -> pd.DataFrame:
        """Get comprehensive zone statistics."""
        
        # Use stored procedure
        stats = db_manager.query_dataframe(
            "CALL CalculateZoneDwellTime(%s, %s)",
            (start_time, end_time)
        )
        
        if stats.empty:
            return pd.DataFrame()
        
        # Add additional metrics
        stats['occupancy_time_min'] = stats['total_minutes']
        stats['avg_occupancy'] = stats['total_minutes'] / stats['unique_agvs']
        
        # Merge with zone information
        stats = stats.merge(
            self.zones[['zone_id', 'category', 'zone_type', 'max_agvs']],
            on='zone_id',
            how='left'
        )
        
        # Calculate utilization
        stats['utilization'] = (stats['unique_agvs'] / stats['max_agvs'] * 100).fillna(0)
        
        return stats
    
    def get_zone_transitions(self, start_time: datetime, 
                           end_time: datetime) -> pd.DataFrame:
        """Analyze zone-to-zone transitions."""
        
        transitions = db_manager.query_dataframe(
            "CALL GenerateZoneTransitionMatrix(%s, %s)",
            (start_time, end_time)
        )
        
        if not transitions.empty:
            # Build transition graph
            self._build_transition_graph(transitions)
        
        return transitions
    
    def _build_transition_graph(self, transitions: pd.DataFrame):
        """Build network graph of zone transitions."""
        self.transition_graph.clear()
        
        for _, row in transitions.iterrows():
            self.transition_graph.add_edge(
                row['from_zone'],
                row['to_zone'],
                weight=row['transition_count']
            )
    
    def find_bottlenecks(self, time_window: timedelta = timedelta(hours=1)) -> List[Dict]:
        """Identify zone bottlenecks."""
        
        end_time = datetime.now()
        start_time = end_time - time_window
        
        # Get current zone occupancy
        occupancy = db_manager.query_dataframe("""
            SELECT 
                zone_id,
                COUNT(DISTINCT agv_id) as current_agvs,
                AVG(speed_mps) as avg_speed
            FROM agv_positions
            WHERE ts >= %s
            AND zone_id IS NOT NULL
            GROUP BY zone_id
        """, (start_time,))
        
        if occupancy.empty:
            return []
        
        # Merge with zone limits
        occupancy = occupancy.merge(
            self.zones[['zone_id', 'name', 'max_agvs', 'max_speed_mps']],
            on='zone_id'
        )
        
        bottlenecks = []
        
        for _, zone in occupancy.iterrows():
            # Check occupancy bottleneck
            if zone['current_agvs'] >= zone['max_agvs'] * 0.8:
                bottlenecks.append({
                    'zone_id': zone['zone_id'],
                    'zone_name': zone['name'],
                    'type': 'OCCUPANCY',
                    'severity': 'HIGH' if zone['current_agvs'] >= zone['max_agvs'] else 'MEDIUM',
                    'current_agvs': int(zone['current_agvs']),
                    'max_agvs': int(zone['max_agvs']),
                    'utilization': float(zone['current_agvs'] / zone['max_agvs'] * 100)
                })
            
            # Check speed bottleneck
            if zone['avg_speed'] < zone['max_speed_mps'] * 0.5:
                bottlenecks.append({
                    'zone_id': zone['zone_id'],
                    'zone_name': zone['name'],
                    'type': 'SPEED',
                    'severity': 'MEDIUM',
                    'avg_speed': float(zone['avg_speed']),
                    'max_speed': float(zone['max_speed_mps']),
                    'speed_ratio': float(zone['avg_speed'] / zone['max_speed_mps'] * 100)
                })
        
        return bottlenecks
    
    def calculate_zone_flow(self, time_window: timedelta = timedelta(hours=1)) -> Dict:
        """Calculate flow rates between zones."""
        
        end_time = datetime.now()
        start_time = end_time - time_window
        
        transitions = self.get_zone_transitions(start_time, end_time)
        
        if transitions.empty:
            return {}
        
        # Calculate flow rates
        hours = time_window.total_seconds() / 3600
        
        flow_rates = {}
        for _, row in transitions.iterrows():
            key = f"{row['from_zone']}->{row['to_zone']}"
            flow_rates[key] = {
                'from': row['from_zone'],
                'to': row['to_zone'],
                'flow_rate': row['transition_count'] / hours,
                'unique_agvs': row['unique_agvs']
            }
        
        return flow_rates
    
    def optimize_zone_allocation(self) -> List[Dict]:
        """Suggest zone allocation optimizations."""
        
        suggestions = []
        
        # Analyze historical utilization
        historical = db_manager.query_dataframe("""
            SELECT 
                zone_id,
                AVG(agv_count) as avg_agvs,
                MAX(agv_count) as peak_agvs,
                STD(agv_count) as std_agvs
            FROM (
                SELECT 
                    zone_id,
                    DATE_FORMAT(ts, '%%Y-%%m-%%d %%H:%%i') as minute,
                    COUNT(DISTINCT agv_id) as agv_count
                FROM agv_positions
                WHERE ts >= NOW() - INTERVAL 7 DAY
                AND zone_id IS NOT NULL
                GROUP BY zone_id, minute
            ) as minute_counts
            GROUP BY zone_id
        """)
        
        if historical.empty:
            return suggestions
        
        # Merge with zone info
        analysis = historical.merge(
            self.zones[['zone_id', 'name', 'max_agvs', 'category']],
            on='zone_id'
        )
        
        for _, zone in analysis.iterrows():
            # Under-utilized zones
            if zone['avg_agvs'] < zone['max_agvs'] * 0.3:
                suggestions.append({
                    'zone_id': zone['zone_id'],
                    'zone_name': zone['name'],
                    'type': 'UNDER_UTILIZED',
                    'current_max': int(zone['max_agvs']),
                    'suggested_max': int(max(2, zone['peak_agvs'] * 1.2)),
                    'reason': 'Zone is consistently under-utilized',
                    'potential_savings': 'Reduce allocated resources'
                })
            
            # Over-utilized zones
            elif zone['peak_agvs'] >= zone['max_agvs'] * 0.9:
                suggestions.append({
                    'zone_id': zone['zone_id'],
                    'zone_name': zone['name'],
                    'type': 'OVER_UTILIZED',
                    'current_max': int(zone['max_agvs']),
                    'suggested_max': int(zone['peak_agvs'] * 1.3),
                    'reason': 'Zone frequently reaches capacity',
                    'potential_benefit': 'Reduce congestion and wait times'
                })
            
            # High variability zones
            if zone['std_agvs'] > zone['avg_agvs'] * 0.5:
                suggestions.append({
                    'zone_id': zone['zone_id'],
                    'zone_name': zone['name'],
                    'type': 'HIGH_VARIABILITY',
                    'current_max': int(zone['max_agvs']),
                    'suggested_strategy': 'Implement dynamic allocation',
                    'reason': 'Zone has highly variable demand',
                    'std_deviation': float(zone['std_agvs'])
                })
        
        return suggestions
    
    def get_zone_heatmap_data(self, time_window: timedelta = timedelta(hours=24)) -> Dict:
        """Get zone heatmap data for visualization."""
        
        end_time = datetime.now()
        start_time = end_time - time_window
        
        # Get zone activity
        activity = db_manager.query_dataframe("""
            SELECT 
                z.zone_id,
                z.name,
                z.category,
                z.vertices,
                COUNT(p.id) as activity_count,
                COUNT(DISTINCT p.agv_id) as unique_agvs,
                AVG(p.speed_mps) as avg_speed
            FROM plant_zones z
            LEFT JOIN agv_positions p ON p.zone_id = z.zone_id
                AND p.ts BETWEEN %s AND %s
            WHERE z.active = TRUE
            GROUP BY z.zone_id, z.name, z.category, z.vertices
        """, (start_time, end_time))
        
        if activity.empty:
            return {}
        
        # Normalize activity for heatmap
        max_activity = activity['activity_count'].max()
        
        heatmap_data = {
            'zones': []
        }
        
        for _, zone in activity.iterrows():
            intensity = zone['activity_count'] / max_activity if max_activity > 0 else 0
            
            heatmap_data['zones'].append({
                'zone_id': zone['zone_id'],
                'name': zone['name'],
                'category': zone['category'],
                'vertices': json.loads(zone['vertices']) if zone['vertices'] else [],
                'intensity': float(intensity),
                'activity_count': int(zone['activity_count']),
                'unique_agvs': int(zone['unique_agvs']),
                'avg_speed': float(zone['avg_speed']) if zone['avg_speed'] else 0
            })
        
        return heatmap_data
    
    def predict_zone_demand(self, zone_id: str, 
                           forecast_hours: int = 4) -> pd.DataFrame:
        """Predict future zone demand based on historical patterns."""
        
        # Get historical hourly patterns
        historical = db_manager.query_dataframe("""
            SELECT 
                HOUR(ts) as hour_of_day,
                DAYOFWEEK(ts) as day_of_week,
                COUNT(DISTINCT agv_id) as agv_count
            FROM agv_positions
            WHERE zone_id = %s
            AND ts >= NOW() - INTERVAL 30 DAY
            GROUP BY hour_of_day, day_of_week
        """, (zone_id,))
        
        if historical.empty:
            return pd.DataFrame()
        
        # Simple prediction based on historical average
        current_hour = datetime.now().hour
        current_day = datetime.now().weekday() + 1
        
        predictions = []
        
        for h in range(forecast_hours):
            forecast_hour = (current_hour + h) % 24
            
            # Get historical average for this hour and day
            hist_data = historical[
                (historical['hour_of_day'] == forecast_hour) &
                (historical['day_of_week'] == current_day)
            ]
            
            if not hist_data.empty:
                predicted_agvs = hist_data['agv_count'].mean()
            else:
                # Fall back to hour average
                hour_data = historical[historical['hour_of_day'] == forecast_hour]
                predicted_agvs = hour_data['agv_count'].mean() if not hour_data.empty else 0
            
            predictions.append({
                'forecast_time': datetime.now() + timedelta(hours=h),
                'predicted_agvs': round(predicted_agvs, 1),
                'confidence': 0.8 if not hist_data.empty else 0.5
            })
        
        return pd.DataFrame(predictions)