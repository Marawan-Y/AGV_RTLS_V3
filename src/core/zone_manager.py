"""
Zone management for AGV RTLS system.
"""

import json
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
import numpy as np
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
import yaml

from loguru import logger
from src.core.database import db_manager


class ZoneManager:
    """Manages plant zones and zone-related operations."""
    
    def __init__(self):
        self.zones = {}
        self.zone_polygons = {}
        self.zone_rules = {}
        self.load_zones()
    
    def load_zones(self):
        """Load zones from database and configuration."""
        
        # Load from database
        db_zones = db_manager.execute_query("""
            SELECT 
                zone_id, name, category, zone_type,
                max_speed_mps, max_agvs, priority,
                vertices, centroid_x, centroid_y
            FROM plant_zones
            WHERE active = TRUE
        """)
        
        for zone in db_zones:
            self.zones[zone['zone_id']] = zone
            
            # Create polygon if vertices exist
            if zone['vertices']:
                try:
                    vertices = json.loads(zone['vertices'])
                    self.zone_polygons[zone['zone_id']] = Polygon(vertices)
                except:
                    logger.error(f"Invalid vertices for zone {zone['zone_id']}")
        
        # Load rules from config
        try:
            with open('config/zones_config.yaml', 'r') as f:
                config = yaml.safe_load(f)
                self.zone_rules = config.get('rules', {})
        except Exception as e:
            logger.warning(f"Could not load zone rules: {e}")
        
        logger.info(f"Loaded {len(self.zones)} zones")
    
    def get_zone_at_position(self, x: float, y: float) -> Optional[str]:
        """Get zone ID at given position."""
        point = Point(x, y)
        
        for zone_id, polygon in self.zone_polygons.items():
            if polygon.contains(point):
                return zone_id
        
        return None
    
    def check_zone_rules(self, agv_id: str, zone_id: str, 
                        speed: float = None) -> List[Dict]:
        """Check zone rules for violations."""
        violations = []
        
        if zone_id not in self.zones:
            return violations
        
        zone = self.zones[zone_id]
        
        # Check speed limit
        if speed and zone['max_speed_mps']:
            if speed > zone['max_speed_mps']:
                violations.append({
                    'type': 'SPEED_VIOLATION',
                    'zone_id': zone_id,
                    'agv_id': agv_id,
                    'current_speed': speed,
                    'max_speed': zone['max_speed_mps'],
                    'severity': 'WARNING'
                })
        
        # Check zone type restrictions
        if zone['zone_type'] == 'RESTRICTED':
            # Check if AGV is authorized
            authorized = self._check_authorization(agv_id, zone_id)
            if not authorized:
                violations.append({
                    'type': 'UNAUTHORIZED_ACCESS',
                    'zone_id': zone_id,
                    'agv_id': agv_id,
                    'severity': 'CRITICAL'
                })
        
        # Check occupancy limit
        current_occupancy = self._get_zone_occupancy(zone_id)
        if current_occupancy >= zone['max_agvs']:
            violations.append({
                'type': 'ZONE_FULL',
                'zone_id': zone_id,
                'agv_id': agv_id,
                'current_occupancy': current_occupancy,
                'max_occupancy': zone['max_agvs'],
                'severity': 'WARNING'
            })
        
        return violations
    
    def _check_authorization(self, agv_id: str, zone_id: str) -> bool:
        """Check if AGV is authorized for zone."""
        # Simplified authorization check
        # In production, this would check against access control lists
        
        zone = self.zones.get(zone_id, {})
        
        # Maintenance zones require special authorization
        if zone.get('zone_type') == 'MAINTENANCE':
            result = db_manager.execute_query("""
                SELECT status FROM agv_registry
                WHERE agv_id = %s
            """, (agv_id,))
            
            if result and result[0]['status'] == 'MAINTENANCE':
                return True
            return False
        
        return True
    
    def _get_zone_occupancy(self, zone_id: str) -> int:
        """Get current zone occupancy."""
        result = db_manager.execute_query("""
            SELECT COUNT(DISTINCT agv_id) as count
            FROM agv_positions
            WHERE zone_id = %s
            AND ts >= NOW() - INTERVAL 10 SECOND
        """, (zone_id,))
        
        return result[0]['count'] if result else 0
    
    def get_adjacent_zones(self, zone_id: str) -> List[str]:
        """Get zones adjacent to given zone."""
        adjacent = []
        
        if zone_id not in self.zone_polygons:
            return adjacent
        
        zone_poly = self.zone_polygons[zone_id]
        
        for other_id, other_poly in self.zone_polygons.items():
            if other_id != zone_id:
                if zone_poly.touches(other_poly):
                    adjacent.append(other_id)
        
        return adjacent
    
    def calculate_zone_distance(self, zone1_id: str, zone2_id: str) -> float:
        """Calculate distance between zone centroids."""
        
        if zone1_id not in self.zones or zone2_id not in self.zones:
            return float('inf')
        
        zone1 = self.zones[zone1_id]
        zone2 = self.zones[zone2_id]
        
        if zone1['centroid_x'] and zone2['centroid_x']:
            return np.sqrt(
                (zone1['centroid_x'] - zone2['centroid_x'])**2 +
                (zone1['centroid_y'] - zone2['centroid_y'])**2
            )
        
        return float('inf')
    
    def find_path_zones(self, start_zone: str, end_zone: str) -> List[str]:
        """Find zones along path from start to end (simplified)."""
        
        # This is a simplified version
        # In production, would use proper pathfinding algorithm
        
        if start_zone == end_zone:
            return [start_zone]
        
        # Get adjacent zones and build simple path
        visited = set()
        queue = [(start_zone, [start_zone])]
        
        while queue:
            current, path = queue.pop(0)
            
            if current == end_zone:
                return path
            
            if current in visited:
                continue
            
            visited.add(current)
            
            for adjacent in self.get_adjacent_zones(current):
                if adjacent not in visited:
                    queue.append((adjacent, path + [adjacent]))
        
        return []
    
    def get_zone_info(self, zone_id: str) -> Optional[Dict]:
        """Get detailed zone information."""
        return self.zones.get(zone_id)
    
    def get_all_zones(self) -> Dict[str, Dict]:
        """Get all zones."""
        return self.zones
    
    def create_zone(self, zone_data: Dict) -> bool:
        """Create a new zone."""
        try:
            # Insert into database
            db_manager.execute_query("""
                INSERT INTO plant_zones 
                (zone_id, name, category, zone_type, max_speed_mps, 
                 max_agvs, priority, vertices, centroid_x, centroid_y)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                zone_data['zone_id'],
                zone_data['name'],
                zone_data['category'],
                zone_data.get('zone_type', 'OPERATIONAL'),
                zone_data.get('max_speed_mps', 2.0),
                zone_data.get('max_agvs', 5),
                zone_data.get('priority', 5),
                json.dumps(zone_data.get('vertices', [])),
                zone_data.get('centroid_x'),
                zone_data.get('centroid_y')
            ))
            
            # Reload zones
            self.load_zones()
            
            logger.info(f"Created zone {zone_data['zone_id']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create zone: {e}")
            return False
    
    def update_zone(self, zone_id: str, updates: Dict) -> bool:
        """Update zone properties."""
        try:
            # Build update query
            set_clauses = []
            params = []
            
            for key, value in updates.items():
                if key in ['name', 'category', 'zone_type', 'max_speed_mps', 
                          'max_agvs', 'priority']:
                    set_clauses.append(f"{key} = %s")
                    params.append(value)
            
            if not set_clauses:
                return False
            
            params.append(zone_id)
            
            db_manager.execute_query(
                f"UPDATE plant_zones SET {', '.join(set_clauses)} WHERE zone_id = %s",
                tuple(params)
            )
            
            # Reload zones
            self.load_zones()
            
            logger.info(f"Updated zone {zone_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update zone: {e}")
            return False
    
    def delete_zone(self, zone_id: str) -> bool:
        """Delete a zone (soft delete)."""
        try:
            db_manager.execute_query("""
                UPDATE plant_zones 
                SET active = FALSE 
                WHERE zone_id = %s
            """, (zone_id,))
            
            # Remove from memory
            if zone_id in self.zones:
                del self.zones[zone_id]
            if zone_id in self.zone_polygons:
                del self.zone_polygons[zone_id]
            
            logger.info(f"Deleted zone {zone_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete zone: {e}")
            return False