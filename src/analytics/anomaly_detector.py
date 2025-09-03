"""
Anomaly detection for AGV behavior.
Detects unusual patterns, violations, and potential issues.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from collections import deque
import json
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

from loguru import logger
from src.core.database import db_manager


class AnomalyDetector:
    """Detects anomalies in AGV behavior using multiple methods."""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
        self.history = {}  # AGV-specific history
        self.models = {}  # ML models per AGV
        self.scaler = StandardScaler()
        self.anomaly_buffer = deque(maxlen=1000)
        self._initialize_models()
    
    def _default_config(self) -> Dict:
        """Default anomaly detection configuration."""
        return {
            'speed_threshold': 5.0,  # m/s
            'acceleration_threshold': 3.0,  # m/s²
            'idle_threshold': 300,  # seconds
            'quality_threshold': 0.3,
            'battery_threshold': 15,  # percent
            'zone_violation_enabled': True,
            'collision_threshold': 2.0,  # meters
            'ml_enabled': True,
            'ml_contamination': 0.1,  # Expected anomaly rate
            'history_window': 100,  # Number of points to keep
            'detection_methods': [
                'threshold',
                'statistical',
                'ml_isolation',
                'pattern'
            ]
        }
    
    def _initialize_models(self):
        """Initialize ML models for anomaly detection."""
        if self.config['ml_enabled']:
            # Initialize Isolation Forest for each AGV type
            self.isolation_forest = IsolationForest(
                contamination=self.config['ml_contamination'],
                random_state=42,
                n_estimators=100
            )
    
    def check(self, data: Dict) -> bool:
        """
        Check if data point is anomalous.
        
        Args:
            data: AGV position data
            
        Returns:
            True if anomaly detected
        """
        anomalies = []
        agv_id = data.get('agv_id')
        
        # Initialize history for new AGV
        if agv_id not in self.history:
            self.history[agv_id] = deque(maxlen=self.config['history_window'])
        
        # Add to history
        self.history[agv_id].append(data)
        
        # Run detection methods
        if 'threshold' in self.config['detection_methods']:
            anomalies.extend(self._threshold_detection(data))
        
        if 'statistical' in self.config['detection_methods']:
            anomalies.extend(self._statistical_detection(data, agv_id))
        
        if 'ml_isolation' in self.config['detection_methods'] and self.config['ml_enabled']:
            anomalies.extend(self._ml_detection(data, agv_id))
        
        if 'pattern' in self.config['detection_methods']:
            anomalies.extend(self._pattern_detection(data, agv_id))
        
        # Log anomalies
        if anomalies:
            self._log_anomalies(data, anomalies)
            return True
        
        return False
    
    def _threshold_detection(self, data: Dict) -> List[Dict]:
        """Simple threshold-based anomaly detection."""
        anomalies = []
        
        # Speed check
        speed = data.get('speed_mps', 0)
        if speed > self.config['speed_threshold']:
            anomalies.append({
                'type': 'SPEED_VIOLATION',
                'severity': 'WARNING',
                'value': speed,
                'threshold': self.config['speed_threshold'],
                'message': f"Speed {speed:.2f} m/s exceeds threshold"
            })
        
        # Quality check
        quality = data.get('quality', 1.0)
        if quality < self.config['quality_threshold']:
            anomalies.append({
                'type': 'LOW_SIGNAL_QUALITY',
                'severity': 'WARNING',
                'value': quality,
                'threshold': self.config['quality_threshold'],
                'message': f"Signal quality {quality:.2f} below threshold"
            })
        
        # Battery check
        battery = data.get('battery_percent', 100)
        if battery < self.config['battery_threshold']:
            anomalies.append({
                'type': 'LOW_BATTERY',
                'severity': 'WARNING' if battery > 10 else 'CRITICAL',
                'value': battery,
                'threshold': self.config['battery_threshold'],
                'message': f"Battery level {battery}% is low"
            })
        
        return anomalies
    
    def _statistical_detection(self, data: Dict, agv_id: str) -> List[Dict]:
        """Statistical anomaly detection using z-scores."""
        anomalies = []
        
        if len(self.history[agv_id]) < 10:
            return anomalies
        
        # Extract historical values
        history_df = pd.DataFrame(list(self.history[agv_id]))
        
        # Calculate z-scores for numerical fields
        numerical_fields = ['speed_mps', 'heading_deg', 'quality']
        
        for field in numerical_fields:
            if field in history_df.columns:
                values = history_df[field].dropna()
                if len(values) > 3:
                    z_score = np.abs(stats.zscore(values))[-1]
                    
                    if z_score > 3:  # 3 standard deviations
                        anomalies.append({
                            'type': 'STATISTICAL_ANOMALY',
                            'severity': 'INFO',
                            'field': field,
                            'z_score': float(z_score),
                            'value': data.get(field),
                            'message': f"Unusual {field} value detected (z-score: {z_score:.2f})"
                        })
        
        # Check for acceleration anomalies
        if 'speed_mps' in history_df.columns and len(history_df) > 1:
            speeds = history_df['speed_mps'].values
            accelerations = np.diff(speeds) * self.config.get('sample_rate_hz', 3)
            
            if len(accelerations) > 0:
                current_accel = accelerations[-1]
                if abs(current_accel) > self.config['acceleration_threshold']:
                    anomalies.append({
                        'type': 'ACCELERATION_ANOMALY',
                        'severity': 'WARNING',
                        'value': float(current_accel),
                        'threshold': self.config['acceleration_threshold'],
                        'message': f"High acceleration detected: {current_accel:.2f} m/s²"
                    })
        
        return anomalies
    
    def _ml_detection(self, data: Dict, agv_id: str) -> List[Dict]:
        """Machine learning based anomaly detection."""
        anomalies = []
        
        if len(self.history[agv_id]) < 20:
            return anomalies
        
        try:
            # Prepare features
            features = self._extract_features(data)
            
            if features is not None:
                # Ensure we have a trained model
                if agv_id not in self.models:
                    self._train_model(agv_id)
                
                if agv_id in self.models:
                    # Predict
                    features_scaled = self.models[agv_id]['scaler'].transform([features])
                    prediction = self.models[agv_id]['model'].predict(features_scaled)
                    
                    if prediction[0] == -1:  # Anomaly
                        score = self.models[agv_id]['model'].score_samples(features_scaled)[0]
                        anomalies.append({
                            'type': 'ML_ANOMALY',
                            'severity': 'INFO',
                            'score': float(score),
                            'message': f"ML model detected unusual pattern (score: {score:.3f})"
                        })
        
        except Exception as e:
            logger.debug(f"ML detection error: {e}")
        
        return anomalies
    
    def _pattern_detection(self, data: Dict, agv_id: str) -> List[Dict]:
        """Detect specific movement patterns."""
        anomalies = []
        
        if len(self.history[agv_id]) < 30:
            return anomalies
        
        history_df = pd.DataFrame(list(self.history[agv_id]))
        
        # Check for idle time
        if 'speed_mps' in history_df.columns:
            recent_speeds = history_df['speed_mps'].tail(30).values
            idle_count = np.sum(recent_speeds < 0.1)
            idle_seconds = idle_count / self.config.get('sample_rate_hz', 3)
            
            if idle_seconds > self.config['idle_threshold']:
                anomalies.append({
                    'type': 'EXCESSIVE_IDLE',
                    'severity': 'WARNING',
                    'idle_time': float(idle_seconds),
                    'threshold': self.config['idle_threshold'],
                    'message': f"AGV idle for {idle_seconds:.0f} seconds"
                })
        
        # Check for circular movement (stuck)
        if 'plant_x' in history_df.columns and 'plant_y' in history_df.columns:
            positions = history_df[['plant_x', 'plant_y']].tail(20).values
            if len(positions) > 10:
                # Calculate total distance vs displacement
                total_distance = np.sum(np.linalg.norm(np.diff(positions, axis=0), axis=1))
                displacement = np.linalg.norm(positions[-1] - positions[0])
                
                if total_distance > 0 and displacement / total_distance < 0.2:
                    anomalies.append({
                        'type': 'CIRCULAR_MOVEMENT',
                        'severity': 'WARNING',
                        'total_distance': float(total_distance),
                        'displacement': float(displacement),
                        'message': "AGV appears to be moving in circles"
                    })
        
        # Check for erratic heading changes
        if 'heading_deg' in history_df.columns:
            headings = history_df['heading_deg'].tail(10).values
            heading_changes = np.abs(np.diff(headings))
            # Handle wrap-around
            heading_changes = np.minimum(heading_changes, 360 - heading_changes)
            
            if np.mean(heading_changes) > 45:  # Average change > 45 degrees
                anomalies.append({
                    'type': 'ERRATIC_HEADING',
                    'severity': 'INFO',
                    'avg_change': float(np.mean(heading_changes)),
                    'message': "Erratic heading changes detected"
                })
        
        return anomalies
    
    def _extract_features(self, data: Dict) -> Optional[np.ndarray]:
        """Extract features for ML model."""
        try:
            features = [
                data.get('speed_mps', 0),
                data.get('heading_deg', 0),
                data.get('quality', 1.0),
                data.get('battery_percent', 100),
                data.get('plant_x', 0),
                data.get('plant_y', 0)
            ]
            return np.array(features)
        except:
            return None
    
    def _train_model(self, agv_id: str):
        """Train ML model for specific AGV."""
        try:
            history_df = pd.DataFrame(list(self.history[agv_id]))
            
            # Extract features
            feature_cols = ['speed_mps', 'heading_deg', 'quality', 
                          'battery_percent', 'plant_x', 'plant_y']
            
            available_cols = [col for col in feature_cols if col in history_df.columns]
            
            if len(available_cols) >= 3:
                X = history_df[available_cols].fillna(0).values
                
                # Scale features
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                # Train Isolation Forest
                model = IsolationForest(
                    contamination=self.config['ml_contamination'],
                    random_state=42
                )
                model.fit(X_scaled)
                
                self.models[agv_id] = {
                    'model': model,
                    'scaler': scaler,
                    'features': available_cols
                }
                
                logger.debug(f"Trained ML model for {agv_id}")
        
        except Exception as e:
            logger.debug(f"Failed to train model for {agv_id}: {e}")
    
    def _log_anomalies(self, data: Dict, anomalies: List[Dict]):
        """Log detected anomalies to database."""
        try:
            for anomaly in anomalies:
                # Add to buffer
                self.anomaly_buffer.append({
                    'timestamp': datetime.now(),
                    'agv_id': data.get('agv_id'),
                    'anomaly': anomaly
                })
                
                # Log to database
                db_manager.execute_query("""
                    INSERT INTO system_events 
                    (event_type, severity, agv_id, zone_id, message, details)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    anomaly['type'],
                    anomaly['severity'],
                    data.get('agv_id'),
                    data.get('zone_id'),
                    anomaly['message'],
                    json.dumps({**anomaly, **{'position': [data.get('plant_x'), data.get('plant_y')]}})
                ))
                
                logger.info(f"Anomaly detected for {data.get('agv_id')}: {anomaly['type']}")
        
        except Exception as e:
            logger.error(f"Failed to log anomaly: {e}")
    
    def detect_collision_risk(self, fleet_positions: pd.DataFrame) -> List[Dict]:
        """Detect potential collision risks between AGVs."""
        collision_risks = []
        
        if len(fleet_positions) < 2:
            return collision_risks
        
        # Calculate pairwise distances
        for i in range(len(fleet_positions)):
            for j in range(i + 1, len(fleet_positions)):
                agv1 = fleet_positions.iloc[i]
                agv2 = fleet_positions.iloc[j]
                
                distance = np.sqrt(
                    (agv1['plant_x'] - agv2['plant_x'])**2 +
                    (agv1['plant_y'] - agv2['plant_y'])**2
                )
                
                if distance < self.config['collision_threshold']:
                    # Calculate time to collision based on velocities
                    v1 = np.array([
                        agv1['speed_mps'] * np.cos(np.radians(agv1['heading_deg'])),
                        agv1['speed_mps'] * np.sin(np.radians(agv1['heading_deg']))
                    ])
                    v2 = np.array([
                        agv2['speed_mps'] * np.cos(np.radians(agv2['heading_deg'])),
                        agv2['speed_mps'] * np.sin(np.radians(agv2['heading_deg']))
                    ])
                    
                    relative_velocity = np.linalg.norm(v1 - v2)
                    
                    if relative_velocity > 0:
                        time_to_collision = distance / relative_velocity
                        
                        if time_to_collision < 5:  # Less than 5 seconds
                            collision_risks.append({
                                'agv1': agv1['agv_id'],
                                'agv2': agv2['agv_id'],
                                'distance': float(distance),
                                'time_to_collision': float(time_to_collision),
                                'severity': 'CRITICAL' if time_to_collision < 2 else 'WARNING'
                            })
        
        return collision_risks
    
    def get_anomaly_statistics(self, time_window: timedelta = timedelta(hours=24)) -> Dict:
        """Get anomaly statistics for dashboard."""
        cutoff_time = datetime.now() - time_window
        
        # Query database for recent anomalies
        result = db_manager.query_dataframe("""
            SELECT 
                event_type,
                severity,
                COUNT(*) as count,
                COUNT(DISTINCT agv_id) as affected_agvs
            FROM system_events
            WHERE created_at >= %s
            AND event_type IN (
                'SPEED_VIOLATION', 'ZONE_VIOLATION', 'ANOMALY_DETECTED',
                'COLLISION_RISK', 'LOW_BATTERY', 'EXCESSIVE_IDLE'
            )
            GROUP BY event_type, severity
        """, (cutoff_time,))
        
        # Calculate statistics
        stats = {
            'total_anomalies': int(result['count'].sum()) if not result.empty else 0,
            'affected_agvs': int(result['affected_agvs'].sum()) if not result.empty else 0,
            'by_type': result.groupby('event_type')['count'].sum().to_dict() if not result.empty else {},
            'by_severity': result.groupby('severity')['count'].sum().to_dict() if not result.empty else {},
            'recent_buffer': len(self.anomaly_buffer)
        }
        
        return stats