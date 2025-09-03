"""Coordinate transformation and zone management."""

import os
import json
import numpy as np
from typing import Tuple, Optional, Dict, Any, List
from pathlib import Path
import pickle

from pyproj import Transformer
from shapely.geometry import Point, Polygon, shape
from shapely.ops import transform
import geopandas as gpd
from loguru import logger


class TransformManager:
    """Manages coordinate transformations and zone detection."""
    
    def __init__(self):
        self.config = self._load_config()
        self.transformer = self._init_transformer()
        self.affine_matrix = self._load_affine()
        self.zones = self._load_zones()
        self.cache = {}
    
    def _load_config(self) -> Dict[str, Any]:
        """Load transformation configuration."""
        return {
            'utm_epsg': int(os.getenv('UTM_EPSG', 32633)),
            'transform_mode': os.getenv('TRANSFORM_MODE', 'auto'),
            'plant_crs': os.getenv('PLANT_CRS', 'LOCAL'),
            'plant_bounds': {
                'xmin': float(os.getenv('PLANT_XMIN', 0)),
                'xmax': float(os.getenv('PLANT_XMAX', 200)),
                'ymin': float(os.getenv('PLANT_YMIN', 0)),
                'ymax': float(os.getenv('PLANT_YMAX', 150))
            }
        }
    
    def _init_transformer(self) -> Transformer:
        """Initialize coordinate transformer."""
        return Transformer.from_crs(
            "EPSG:4326",  # WGS84
            f"EPSG:{self.config['utm_epsg']}",
            always_xy=True
        )
    
    def _load_affine(self) -> Optional[np.ndarray]:
        """Load affine transformation matrix."""
        affine_paths = [
            Path('assets/calibration/affine_matrix.npy'),
            Path('config/affine_matrix.npy')
        ]
        
        for path in affine_paths:
            if path.exists():
                try:
                    matrix = np.load(path)
                    logger.info(f"Loaded affine matrix from {path}")
                    return matrix
                except Exception as e:
                    logger.error(f"Failed to load affine matrix: {e}")
        
        logger.warning("No affine matrix found, using identity transform")
        return np.eye(3)
    
    def _load_zones(self) -> gpd.GeoDataFrame:
        """Load zone definitions."""
        zone_path = Path('assets/zones.geojson')
        
        if zone_path.exists():
            try:
                zones = gpd.read_file(zone_path)
                logger.info(f"Loaded {len(zones)} zones")
                return zones
            except Exception as e:
                logger.error(f"Failed to load zones: {e}")
        
        return gpd.GeoDataFrame()
    
    def to_plant_coords(self, data: Dict[str, Any]) -> Tuple[float, float]:
        """Transform coordinates to plant CRS."""
        
        # Check if plant coordinates already provided
        if 'plant_x' in data and 'plant_y' in data:
            return float(data['plant_x']), float(data['plant_y'])
        
        # Check cache
        cache_key = (data.get('lat'), data.get('lon'))
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Transform WGS84 to UTM
        lat, lon = float(data.get('lat', 0)), float(data.get('lon', 0))
        utm_x, utm_y = self.transformer.transform(lon, lat)
        
        # Apply affine transformation if available
        if self.affine_matrix is not None:
            coords = np.array([utm_x, utm_y, 1.0])
            plant_coords = self.affine_matrix @ coords
            plant_x, plant_y = plant_coords[0], plant_coords[1]
        else:
            plant_x, plant_y = utm_x, utm_y
        
        # Validate bounds
        bounds = self.config['plant_bounds']
        if not (bounds['xmin'] <= plant_x <= bounds['xmax'] and 
                bounds['ymin'] <= plant_y <= bounds['ymax']):
            logger.warning(f"Coordinates out of bounds: ({plant_x}, {plant_y})")
        
        # Cache result
        self.cache[cache_key] = (plant_x, plant_y)
        
        return plant_x, plant_y
    
    def get_zone(self, x: float, y: float) -> Optional[str]:
        """Get zone ID for given coordinates."""
        if self.zones.empty:
            return None
        
        point = Point(x, y)
        
        for idx, zone in self.zones.iterrows():
            if zone.geometry.contains(point):
                return zone.get('zone_id', zone.get('name'))
        
        return None
    
    def calibrate(self, control_points: List[Dict]) -> np.ndarray:
        """Calibrate transformation using control points."""
        
        if len(control_points) < 3:
            raise ValueError("Need at least 3 control points for calibration")
        
        # Extract world and plant coordinates
        world_coords = np.array([
            [p['world_x'], p['world_y'], 1.0] 
            for p in control_points
        ])
        plant_coords = np.array([
            [p['plant_x'], p['plant_y'], 1.0] 
            for p in control_points
        ])
        
        # Compute affine transformation using least squares
        affine_matrix, residuals, rank, s = np.linalg.lstsq(
            world_coords, plant_coords, rcond=None
        )
        
        # Validate transformation
        errors = []
        for wc, pc in zip(world_coords, plant_coords):
            predicted = affine_matrix.T @ wc
            error = np.linalg.norm(predicted[:2] - pc[:2])
            errors.append(error)
        
        mean_error = np.mean(errors)
        logger.info(f"Calibration complete. Mean error: {mean_error:.3f} meters")
        
        if mean_error > 1.0:
            logger.warning("High calibration error detected")
        
        # Save calibration
        save_path = Path('assets/calibration/affine_matrix.npy')
        save_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(save_path, affine_matrix.T)
        
        self.affine_matrix = affine_matrix.T
        return affine_matrix.T