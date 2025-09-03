# 31. src/ingestion/batch_processor.py
"""Batch processing for historical data and bulk imports."""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import json

from loguru import logger
from src.core.database import db_manager
from src.core.transforms import TransformManager
from src.ingestion.data_validator import DataValidator


class BatchProcessor:
    """Processes batch data imports."""
    
    def __init__(self):
        self.transform_manager = TransformManager()
        self.validator = DataValidator()
        self.batch_size = 1000
    
    def process_csv(self, file_path: str, agv_id_column: str = 'agv_id') -> Dict:
        """Process CSV file with AGV data."""
        try:
            df = pd.read_csv(file_path)
            return self._process_dataframe(df, agv_id_column)
        except Exception as e:
            logger.error(f"Failed to process CSV: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def process_parquet(self, file_path: str, agv_id_column: str = 'agv_id') -> Dict:
        """Process Parquet file with AGV data."""
        try:
            df = pd.read_parquet(file_path)
            return self._process_dataframe(df, agv_id_column)
        except Exception as e:
            logger.error(f"Failed to process Parquet: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _process_dataframe(self, df: pd.DataFrame, agv_id_column: str) -> Dict:
        """Process a DataFrame of AGV data."""
        
        processed = 0
        failed = 0
        
        # Prepare batch insert
        insert_query = """
            INSERT INTO agv_positions (
                ts, agv_id, lat, lon, heading_deg, speed_mps,
                quality, plant_x, plant_y, zone_id, battery_percent, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        batch_data = []
        
        for _, row in df.iterrows():
            try:
                # Validate row
                data = row.to_dict()
                if not self.validator.validate(data):
                    failed += 1
                    continue
                
                # Transform coordinates
                plant_x, plant_y = self.transform_manager.to_plant_coords(data)
                zone_id = self.transform_manager.get_zone(plant_x, plant_y)
                
                # Prepare tuple for insertion
                batch_data.append((
                    pd.to_datetime(data.get('ts', datetime.now())),
                    data.get(agv_id_column),
                    data.get('lat'),
                    data.get('lon'),
                    data.get('heading_deg'),
                    data.get('speed_mps'),
                    data.get('quality', 1.0),
                    plant_x,
                    plant_y,
                    zone_id,
                    data.get('battery_percent', 100),
                    data.get('status', 'ACTIVE')
                ))
                
                processed += 1
                
                # Insert batch when threshold reached
                if len(batch_data) >= self.batch_size:
                    db_manager.execute_many(insert_query, batch_data)
                    batch_data = []
                    
            except Exception as e:
                logger.error(f"Failed to process row: {e}")
                failed += 1
        
        # Insert remaining data
        if batch_data:
            db_manager.execute_many(insert_query, batch_data)
        
        return {
            'status': 'success',
            'processed': processed,
            'failed': failed,
            'total': len(df)
        }