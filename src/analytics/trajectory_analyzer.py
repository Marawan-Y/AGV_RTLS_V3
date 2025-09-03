"""
Trajectory analysis for AGV movement patterns.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from scipy.interpolate import interp1d
from scipy.signal import savgol_filter
import networkx as nx

from loguru import logger
from src.core.database import db_manager


class TrajectoryAnalyzer:
    """Analyzes AGV trajectories for patterns and insights."""
    
    def __init__(self):
        self.cache = {}
        
    def get_trajectory(self, agv_id: str, start_time: datetime, 
                      end_time: datetime, downsample: int = 1) -> pd.DataFrame:
        """Get AGV trajectory data."""
        
        # Use stored procedure for efficient retrieval
        query = """
            CALL GetAGVTrajectory(%s, %s, %s, %s)
        """
        
        return db_manager.query_dataframe(
            query, (agv_id, start_time, end_time, downsample)
        )
    
    def calculate_stats(self, trajectory: pd.DataFrame) -> Dict:
        """Calculate trajectory statistics."""
        
        if trajectory.empty:
            return self._empty_stats()
        
        # Calculate distances between points
        if 'plant_x' in trajectory.columns and 'plant_y' in trajectory.columns:
            distances = np.sqrt(
                np.diff(trajectory['plant_x'])**2 + 
                np.diff(trajectory['plant_y'])**2
            )
            total_distance = np.sum(distances)
        else:
            total_distance = 0
        
        # Time calculations
        if 'ts' in trajectory.columns:
            trajectory['ts'] = pd.to_datetime(trajectory['ts'])
            duration = (trajectory['ts'].iloc[-1] - trajectory['ts'].iloc[0]).total_seconds()
        else:
            duration = 0
        
        # Speed statistics
        speeds = trajectory['speed_mps'].values if 'speed_mps' in trajectory.columns else []
        
        # Stop time calculation (speed < 0.1 m/s)
        stop_samples = np.sum(speeds < 0.1) if len(speeds) > 0 else 0
        stop_time = stop_samples / 3.0 / 60  # Convert to minutes (3Hz sampling)
        
        stats = {
            'total_distance': round(total_distance, 1),
            'duration_min': round(duration / 60, 1) if duration > 0 else 0,
            'avg_speed': round(np.mean(speeds), 2) if len(speeds) > 0 else 0,
            'max_speed': round(np.max(speeds), 2) if len(speeds) > 0 else 0,
            'min_speed': round(np.min(speeds[speeds > 0]), 2) if len(speeds[speeds > 0]) > 0 else 0,
            'stop_time': round(stop_time, 1),
            'stop_percentage': round(stop_time * 60 / duration * 100, 1) if duration > 0 else 0,
            'total_points': len(trajectory),
            'unique_zones': trajectory['zone_id'].nunique() if 'zone_id' in trajectory.columns else 0
        }
        
        # Add turn statistics
        if 'heading_deg' in trajectory.columns:
            turn_stats = self._calculate_turn_statistics(trajectory['heading_deg'].values)
            stats.update(turn_stats)
        
        return stats
    
    def _empty_stats(self) -> Dict:
        """Return empty statistics structure."""
        return {
            'total_distance': 0,
            'duration_min': 0,
            'avg_speed': 0,
            'max_speed': 0,
            'min_speed': 0,
            'stop_time': 0,
            'stop_percentage': 0,
            'total_points': 0,
            'unique_zones': 0
        }
    
    def _calculate_turn_statistics(self, headings: np.ndarray) -> Dict:
        """Calculate turning statistics from heading data."""
        
        if len(headings) < 2:
            return {'total_turns': 0, 'avg_turn_rate': 0}
        
        # Calculate heading changes
        heading_changes = np.diff(headings)
        
        # Handle wrap-around (e.g., 359° to 1°)
        heading_changes = np.where(heading_changes > 180, heading_changes - 360, heading_changes)
        heading_changes = np.where(heading_changes < -180, heading_changes + 360, heading_changes)
        
        # Count significant turns (> 30 degrees)
        significant_turns = np.sum(np.abs(heading_changes) > 30)
        
        # Calculate turn rate (degrees per second)
        sample_rate = 3  # Hz
        turn_rates = np.abs(heading_changes) * sample_rate
        
        return {
            'total_turns': int(significant_turns),
            'avg_turn_rate': round(np.mean(turn_rates), 1),
            'max_turn_rate': round(np.max(turn_rates), 1)
        }
    
    def smooth_trajectory(self, trajectory: pd.DataFrame, 
                         window_length: int = 11) -> pd.DataFrame:
        """Apply smoothing to trajectory data."""
        
        if len(trajectory) < window_length:
            return trajectory
        
        smoothed = trajectory.copy()
        
        # Smooth position data
        if 'plant_x' in trajectory.columns and 'plant_y' in trajectory.columns:
            smoothed['plant_x'] = savgol_filter(
                trajectory['plant_x'], window_length, 3
            )
            smoothed['plant_y'] = savgol_filter(
                trajectory['plant_y'], window_length, 3
            )
        
        # Smooth speed data
        if 'speed_mps' in trajectory.columns:
            smoothed['speed_mps'] = savgol_filter(
                trajectory['speed_mps'], window_length, 3
            )
        
        return smoothed
    
    def detect_stops(self, trajectory: pd.DataFrame, 
                    speed_threshold: float = 0.1,
                    min_duration: float = 5.0) -> List[Dict]:
        """Detect stop events in trajectory."""
        
        stops = []
        
        if 'speed_mps' not in trajectory.columns:
            return stops
        
        # Find sequences where speed is below threshold
        is_stopped = trajectory['speed_mps'] < speed_threshold
        
        # Find start and end of stop sequences
        stop_starts = np.where(np.diff(is_stopped.astype(int)) == 1)[0] + 1
        stop_ends = np.where(np.diff(is_stopped.astype(int)) == -1)[0] + 1
        
        # Handle edge cases
        if is_stopped.iloc[0]:
            stop_starts = np.insert(stop_starts, 0, 0)
        if is_stopped.iloc[-1]:
            stop_ends = np.append(stop_ends, len(trajectory))
        
        # Create stop events
        for start_idx, end_idx in zip(stop_starts, stop_ends):
            duration = (end_idx - start_idx) / 3.0  # Convert to seconds (3Hz)
            
            if duration >= min_duration:
                stops.append({
                    'start_time': trajectory.iloc[start_idx]['ts'],
                    'end_time': trajectory.iloc[min(end_idx, len(trajectory)-1)]['ts'],
                    'duration_sec': duration,
                    'location': {
                        'x': trajectory.iloc[start_idx]['plant_x'],
                        'y': trajectory.iloc[start_idx]['plant_y']
                    },
                    'zone': trajectory.iloc[start_idx].get('zone_id')
                })
        
        return stops
    
    def calculate_path_efficiency(self, trajectory: pd.DataFrame) -> float:
        """
        Calculate path efficiency (direct distance / actual distance).
        
        Returns value between 0 and 1, where 1 is perfectly efficient.
        """
        
        if len(trajectory) < 2:
            return 1.0
        
        # Calculate actual path distance
        actual_distance = np.sum(np.sqrt(
            np.diff(trajectory['plant_x'])**2 + 
            np.diff(trajectory['plant_y'])**2
        ))
        
        # Calculate direct distance
        direct_distance = np.sqrt(
            (trajectory.iloc[-1]['plant_x'] - trajectory.iloc[0]['plant_x'])**2 +
            (trajectory.iloc[-1]['plant_y'] - trajectory.iloc[0]['plant_y'])**2
        )
        
        if actual_distance == 0:
            return 0.0
        
        return min(1.0, direct_distance / actual_distance)
    
    def find_repeated_paths(self, agv_id: str, 
                           time_window: timedelta = timedelta(days=7),
                           similarity_threshold: float = 0.8) -> List[Dict]:
        """Find repeated path patterns."""
        
        end_time = datetime.now()
        start_time = end_time - time_window
        
        # Get all trajectories in time window
        # Split by stops to identify individual trips
        full_trajectory = self.get_trajectory(agv_id, start_time, end_time)
        
        if full_trajectory.empty:
            return []
        
        # Detect stops to segment trajectory
        stops = self.detect_stops(full_trajectory)
        
        # Extract path segments between stops
        segments = []
        prev_end = 0
        
        for stop in stops:
            stop_start = full_trajectory[full_trajectory['ts'] == stop['start_time']].index[0]
            
            if stop_start > prev_end:
                segment = full_trajectory.iloc[prev_end:stop_start]
                if len(segment) > 10:  # Minimum segment length
                    segments.append(segment)
            
            stop_end = full_trajectory[full_trajectory['ts'] == stop['end_time']].index[0]
            prev_end = stop_end
        
        # Add final segment
        if prev_end < len(full_trajectory):
            segment = full_trajectory.iloc[prev_end:]
            if len(segment) > 10:
                segments.append(segment)
        
        # Compare segments for similarity
        repeated_paths = []
        
        for i in range(len(segments)):
            similar_segments = []
            
            for j in range(i + 1, len(segments)):
                similarity = self._calculate_path_similarity(segments[i], segments[j])
                
                if similarity > similarity_threshold:
                    similar_segments.append({
                        'segment_index': j,
                        'similarity': similarity,
                        'timestamp': segments[j].iloc[0]['ts']
                    })
            
            if similar_segments:
                repeated_paths.append({
                    'base_segment': i,
                    'start_zone': segments[i].iloc[0].get('zone_id'),
                    'end_zone': segments[i].iloc[-1].get('zone_id'),
                    'repetitions': len(similar_segments) + 1,
                    'similar_segments': similar_segments
                })
        
        return repeated_paths
    
    def _calculate_path_similarity(self, path1: pd.DataFrame, 
                                  path2: pd.DataFrame) -> float:
        """Calculate similarity between two path segments."""
        
        # Use Dynamic Time Warping or simpler metric
        # For simplicity, using endpoint and length comparison
        
        # Compare start and end points
        start_dist = np.sqrt(
            (path1.iloc[0]['plant_x'] - path2.iloc[0]['plant_x'])**2 +
            (path1.iloc[0]['plant_y'] - path2.iloc[0]['plant_y'])**2
        )
        
        end_dist = np.sqrt(
            (path1.iloc[-1]['plant_x'] - path2.iloc[-1]['plant_x'])**2 +
            (path1.iloc[-1]['plant_y'] - path2.iloc[-1]['plant_y'])**2
        )
        
        # Compare path lengths
        len1 = self.calculate_stats(path1)['total_distance']
        len2 = self.calculate_stats(path2)['total_distance']
        
        if max(len1, len2) == 0:
            return 0
        
        length_similarity = min(len1, len2) / max(len1, len2)
        
        # Combine metrics
        endpoint_similarity = 1 - (start_dist + end_dist) / 100  # Normalize by typical distance
        endpoint_similarity = max(0, endpoint_similarity)
        
        return (endpoint_similarity + length_similarity) / 2
    
    def predict_destination(self, agv_id: str, 
                          current_trajectory: pd.DataFrame) -> Optional[str]:
        """Predict likely destination based on historical patterns."""
        
        if len(current_trajectory) < 5:
            return None
        
        # Get historical destinations from similar starting points
        current_start = current_trajectory.iloc[0]
        
        historical = db_manager.query_dataframe("""
            SELECT 
                destination_zone_id,
                COUNT(*) as frequency
            FROM agv_tasks
            WHERE agv_id = %s
            AND origin_zone_id = %s
            AND status = 'COMPLETED'
            AND completed_at >= NOW() - INTERVAL 30 DAY
            GROUP BY destination_zone_id
            ORDER BY frequency DESC
            LIMIT 1
        """, (agv_id, current_start.get('zone_id')))
        
        if not historical.empty:
            return historical.iloc[0]['destination_zone_id']
        
        return None