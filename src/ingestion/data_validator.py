# 33. src/ingestion/data_validator.py
"""Data validation for incoming AGV data."""

from typing import Dict, Any, Optional
import jsonschema
from datetime import datetime

from loguru import logger


class DataValidator:
    """Validates incoming AGV data."""
    
    def __init__(self):
        self.schema = self._get_schema()
        self.stats = {
            'valid': 0,
            'invalid': 0
        }
    
    def _get_schema(self) -> Dict:
        """Get JSON schema for validation."""
        return {
            "type": "object",
            "required": ["agv_id"],
            "properties": {
                "agv_id": {"type": "string", "minLength": 1},
                "ts": {"type": "string"},
                "lat": {"type": "number", "minimum": -90, "maximum": 90},
                "lon": {"type": "number", "minimum": -180, "maximum": 180},
                "plant_x": {"type": "number"},
                "plant_y": {"type": "number"},
                "heading_deg": {"type": "number", "minimum": 0, "maximum": 360},
                "speed_mps": {"type": "number", "minimum": 0},
                "quality": {"type": "number", "minimum": 0, "maximum": 1},
                "battery_percent": {"type": "number", "minimum": 0, "maximum": 100},
                "status": {"type": "string"}
            }
        }
    
    def validate(self, data: Dict) -> bool:
        """Validate data against schema."""
        try:
            # Basic schema validation
            jsonschema.validate(data, self.schema)
            
            # Additional business rules
            if not self._validate_business_rules(data):
                self.stats['invalid'] += 1
                return False
            
            self.stats['valid'] += 1
            return True
            
        except jsonschema.ValidationError as e:
            logger.debug(f"Validation error: {e}")
            self.stats['invalid'] += 1
            return False
    
    def _validate_business_rules(self, data: Dict) -> bool:
        """Validate business rules."""
        
        # Check timestamp freshness if provided
        if 'ts' in data:
            try:
                ts = datetime.fromisoformat(data['ts'].replace('Z', '+00:00'))
                age = (datetime.now(ts.tzinfo) - ts).total_seconds()
                if age > 3600:  # Reject data older than 1 hour
                    logger.debug(f"Data too old: {age} seconds")
                    return False
            except:
                pass
        
        # Check speed limits
        if 'speed_mps' in data and data['speed_mps'] > 10:
            logger.debug(f"Speed too high: {data['speed_mps']} m/s")
            return False
        
        return True
    
    def get_stats(self) -> Dict:
        """Get validation statistics."""
        total = self.stats['valid'] + self.stats['invalid']
        return {
            **self.stats,
            'total': total,
            'valid_rate': self.stats['valid'] / total if total > 0 else 0
        }