"""
Heatmap generation for AGV position visualization.
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple, Optional, List
from datetime import datetime, timedelta
from scipy.ndimage import gaussian_filter
import datashader as ds
import datashader.transfer_functions as tf
from datashader.colors import viridis

from loguru import logger
from src.core.database import db_manager


class HeatmapGenerator:
    """Generates heatmaps for AGV position density visualization."""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
        self.cache = {}
        
    def _default_config(self) -> Dict:
        """Default heatmap configuration."""
        return {
            'bins': 150,
            'smoothing': 1.0,
            'min_samples': 10,
            'cache_ttl': 300,  # seconds
            'use_datashader': True,
            'colormap': 'viridis',
            'bounds': {
                'xmin': 0, 'xmax': 200,
                'ymin': 0, 'ymax': 150
            }
        }
    
    def generate(self, start_time: datetime, end_time: datetime,
                agv_ids: Optional[List[str]] = None,
                zone_id: Optional[str] = None) -> Dict:
        """
        Generate heatmap data for given time range.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            agv_ids: Optional list of AGV IDs to include
            zone_id: Optional zone filter
            
        Returns:
            Dictionary with heatmap data
        """
        # Check cache
        cache_key = f"{start_time}_{end_time}_{agv_ids}_{zone_id}"
        if cache_key in self.cache:
            cached_time, cached_data = self.cache[cache_key]
            if (datetime.now() - cached_time).seconds < self.config['cache_ttl']:
                return cached_data
        
        # Query positions
        positions = self._get_positions(start_time, end_time, agv_ids, zone_id)
        
        if positions.empty:
            return None
        
        # Generate heatmap
        if self.config['use_datashader'] and len(positions) > 1000:
            heatmap_data = self._generate_datashader(positions)
        else:
            heatmap_data = self._generate_numpy(positions)
        
        # Cache result
        self.cache[cache_key] = (datetime.now(), heatmap_data)
        
        return heatmap_data
    
    def _get_positions(self, start_time: datetime, end_time: datetime,
                      agv_ids: Optional[List[str]] = None,
                      zone_id: Optional[str] = None) -> pd.DataFrame:
        """Query positions from database."""
        query = """
            SELECT plant_x, plant_y, speed_mps, agv_id, zone_id
            FROM agv_positions
            WHERE ts BETWEEN %s AND %s
        """
        params = [start_time, end_time]
        
        if agv_ids:
            placeholders = ','.join(['%s'] * len(agv_ids))
            query += f" AND agv_id IN ({placeholders})"
            params.extend(agv_ids)
        
        if zone_id:
            query += " AND zone_id = %s"
            params.append(zone_id)
        
        # Add sampling for large datasets
        query += " ORDER BY ts"
        
        return db_manager.query_dataframe(query, tuple(params))
    
    def _generate_numpy(self, positions: pd.DataFrame) -> Dict:
        """Generate heatmap using NumPy."""
        bounds = self.config['bounds']
        bins = self.config['bins']
        
        # Create 2D histogram
        H, xedges, yedges = np.histogram2d(
            positions['plant_x'],
            positions['plant_y'],
            bins=bins,
            range=[[bounds['xmin'], bounds['xmax']], 
                   [bounds['ymin'], bounds['ymax']]]
        )
        
        # Apply Gaussian smoothing
        if self.config['smoothing'] > 0:
            H = gaussian_filter(H, sigma=self.config['smoothing'])
        
        # Normalize
        if H.max() > 0:
            H = H / H.max()
        
        return {
            'z': H.T.tolist(),  # Transpose for correct orientation
            'x': xedges.tolist(),
            'y': yedges.tolist(),
            'type': 'numpy',
            'samples': len(positions)
        }
    
    def _generate_datashader(self, positions: pd.DataFrame) -> Dict:
        """Generate heatmap using Datashader for large datasets."""
        bounds = self.config['bounds']
        
        # Create canvas
        canvas = ds.Canvas(
            plot_width=self.config['bins'],
            plot_height=self.config['bins'],
            x_range=(bounds['xmin'], bounds['xmax']),
            y_range=(bounds['ymin'], bounds['ymax'])
        )
        
        # Aggregate points
        agg = canvas.points(positions, 'plant_x', 'plant_y')
        
        # Apply transfer function
        img = tf.shade(agg, cmap=viridis, how='log')
        
        # Convert to array
        img_array = np.array(img.to_pil())
        
        # Extract single channel for heatmap
        heatmap = img_array[:, :, 0] / 255.0
        
        return {
            'z': heatmap.tolist(),
            'x': np.linspace(bounds['xmin'], bounds['xmax'], self.config['bins']).tolist(),
            'y': np.linspace(bounds['ymin'], bounds['ymax'], self.config['bins']).tolist(),
            'type': 'datashader',
            'samples': len(positions)
        }
    
    def generate_zone_heatmap(self, time_window: timedelta = timedelta(hours=24)) -> Dict:
        """Generate heatmap of zone occupancy."""
        end_time = datetime.now()
        start_time = end_time - time_window
        
        # Query zone occupancy
        result = db_manager.query_dataframe("""
            SELECT 
                z.zone_id,
                z.name,
                z.centroid_x,
                z.centroid_y,
                COUNT(p.id) as sample_count,
                COUNT(DISTINCT p.agv_id) as unique_agvs
            FROM plant_zones z
            LEFT JOIN agv_positions p ON p.zone_id = z.zone_id
                AND p.ts BETWEEN %s AND %s
            WHERE z.active = TRUE
            GROUP BY z.zone_id, z.name, z.centroid_x, z.centroid_y
        """, (start_time, end_time))
        
        if result.empty:
            return None
        
        # Create zone intensity map
        bounds = self.config['bounds']
        bins = 50  # Lower resolution for zones
        
        # Create empty grid
        grid = np.zeros((bins, bins))
        
        # Add zone intensities
        for _, zone in result.iterrows():
            if zone['centroid_x'] and zone['centroid_y']:
                # Convert to grid coordinates
                x_idx = int((zone['centroid_x'] - bounds['xmin']) / 
                           (bounds['xmax'] - bounds['xmin']) * bins)
                y_idx = int((zone['centroid_y'] - bounds['ymin']) / 
                           (bounds['ymax'] - bounds['ymin']) * bins)
                
                if 0 <= x_idx < bins and 0 <= y_idx < bins:
                    # Add Gaussian blob for zone
                    intensity = zone['sample_count'] / 1000  # Normalize
                    for dx in range(-5, 6):
                        for dy in range(-5, 6):
                            nx, ny = x_idx + dx, y_idx + dy
                            if 0 <= nx < bins and 0 <= ny < bins:
                                dist = np.sqrt(dx**2 + dy**2)
                                grid[ny, nx] += intensity * np.exp(-dist**2 / 10)
        
        # Apply smoothing
        grid = gaussian_filter(grid, sigma=2)
        
        # Normalize
        if grid.max() > 0:
            grid = grid / grid.max()
        
        return {
            'z': grid.tolist(),
            'x': np.linspace(bounds['xmin'], bounds['xmax'], bins).tolist(),
            'y': np.linspace(bounds['ymin'], bounds['ymax'], bins).tolist(),
            'type': 'zone_heatmap',
            'zones': result[['zone_id', 'name', 'sample_count']].to_dict('records')
        }
    
    def generate_trajectory_heatmap(self, agv_id: str, 
                                   time_window: timedelta = timedelta(hours=1)) -> Dict:
        """Generate heatmap for single AGV trajectory."""
        end_time = datetime.now()
        start_time = end_time - time_window
        
        positions = self._get_positions(start_time, end_time, [agv_id])
        
        if positions.empty:
            return None
        
        # Use higher resolution for single AGV
        self.config['bins'] = 200
        heatmap = self._generate_numpy(positions)
        
        # Add trajectory statistics
        heatmap['stats'] = {
            'total_points': len(positions),
            'unique_zones': positions['zone_id'].nunique(),
            'avg_speed': positions['speed_mps'].mean(),
            'coverage_area': self._calculate_coverage(positions)
        }
        
        return heatmap
    
    def _calculate_coverage(self, positions: pd.DataFrame) -> float:
        """Calculate area covered by positions."""
        if len(positions) < 3:
            return 0.0
        
        # Create convex hull of positions
        from scipy.spatial import ConvexHull
        
        points = positions[['plant_x', 'plant_y']].values
        
        try:
            hull = ConvexHull(points)
            return float(hull.volume)  # In 2D, volume is area
        except:
            return 0.0
    
    def generate_comparative_heatmap(self, time_periods: List[Tuple[datetime, datetime]],
                                    labels: Optional[List[str]] = None) -> Dict:
        """Generate comparative heatmaps for multiple time periods."""
        heatmaps = []
        
        for i, (start, end) in enumerate(time_periods):
            positions = self._get_positions(start, end)
            
            if not positions.empty:
                heatmap = self._generate_numpy(positions)
                heatmap['label'] = labels[i] if labels else f"Period {i+1}"
                heatmaps.append(heatmap)
        
        # Calculate difference if exactly 2 periods
        if len(heatmaps) == 2:
            diff = np.array(heatmaps[1]['z']) - np.array(heatmaps[0]['z'])
            
            return {
                'heatmaps': heatmaps,
                'difference': {
                    'z': diff.tolist(),
                    'x': heatmaps[0]['x'],
                    'y': heatmaps[0]['y'],
                    'type': 'difference'
                }
            }
        
        return {'heatmaps': heatmaps}