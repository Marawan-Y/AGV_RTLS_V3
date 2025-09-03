"""High-performance MQTT consumer with buffering and error recovery."""

import os
import json
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from collections import deque
from threading import Lock
import signal
import sys

import paho.mqtt.client as mqtt
from asyncio_mqtt import Client as AsyncMQTTClient
from loguru import logger
import numpy as np

from src.core.database import db_manager
from src.core.transforms import TransformManager
from src.ingestion.data_validator import DataValidator
from src.ingestion.buffer_manager import BufferManager
from src.analytics.anomaly_detector import AnomalyDetector


class MQTTConsumer:
    """Production MQTT consumer with advanced features."""
    
    def __init__(self):
        self.config = self._load_config()
        self.transform_manager = TransformManager()
        self.validator = DataValidator()
        self.buffer = BufferManager(max_size=10000)
        self.anomaly_detector = AnomalyDetector()
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'messages_processed': 0,
            'messages_failed': 0,
            'last_message_time': None,
            'start_time': time.time()
        }
        self.stats_lock = Lock()
        
        # MQTT client
        self.client = None
        self.running = False
        self._setup_signal_handlers()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load MQTT configuration."""
        return {
            'broker': os.getenv('MQTT_BROKER', 'localhost'),
            'port': int(os.getenv('MQTT_PORT', 1883)),
            'topic': os.getenv('MQTT_TOPIC', 'rtls/+/position'),
            'qos': int(os.getenv('MQTT_QOS', 1)),
            'username': os.getenv('MQTT_USERNAME'),
            'password': os.getenv('MQTT_PASSWORD'),
            'client_id': os.getenv('MQTT_CLIENT_ID', f'agv_consumer_{os.getpid()}'),
            'keepalive': 30,
            'clean_session': False
        }
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def on_connect(self, client, userdata, flags, rc):
        """Callback for MQTT connection."""
        if rc == 0:
            logger.info(f"Connected to MQTT broker at {self.config['broker']}")
            client.subscribe(self.config['topic'], qos=self.config['qos'])
            logger.info(f"Subscribed to topic: {self.config['topic']}")
        else:
            logger.error(f"Failed to connect, return code: {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """Callback for MQTT disconnection."""
        logger.warning(f"Disconnected from broker, return code: {rc}")
        if rc != 0 and self.running:
            logger.info("Attempting to reconnect...")
            time.sleep(5)
            try:
                client.reconnect()
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
    
    def on_message(self, client, userdata, msg):
        """Process incoming MQTT message."""
        try:
            # Update stats
            with self.stats_lock:
                self.stats['messages_received'] += 1
                self.stats['last_message_time'] = time.time()
            
            # Parse message
            data = json.loads(msg.payload.decode('utf-8'))
            
            # Extract AGV ID from topic or payload
            topic_parts = msg.topic.split('/')
            if len(topic_parts) >= 2:
                data['agv_id'] = data.get('agv_id', topic_parts[1])
            
            # Validate data
            if not self.validator.validate(data):
                logger.warning(f"Invalid data from {data.get('agv_id', 'unknown')}")
                with self.stats_lock:
                    self.stats['messages_failed'] += 1
                return
            
            # Transform coordinates
            plant_x, plant_y = self.transform_manager.to_plant_coords(data)
            data['plant_x'] = plant_x
            data['plant_y'] = plant_y
            
            # Detect zone
            data['zone_id'] = self.transform_manager.get_zone(plant_x, plant_y)
            
            # Check for anomalies
            if self.anomaly_detector.check(data):
                logger.warning(f"Anomaly detected for {data['agv_id']}")
                self._handle_anomaly(data)
            
            # Add to buffer
            self.buffer.add(data)
            
            # Process buffer if threshold reached
            if self.buffer.should_flush():
                self._flush_buffer()
            
            with self.stats_lock:
                self.stats['messages_processed'] += 1
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            with self.stats_lock:
                self.stats['messages_failed'] += 1
    
    def _flush_buffer(self):
        """Flush buffer to database."""
        try:
            batch = self.buffer.get_batch()
            if not batch:
                return
            
            # Prepare batch insert
            insert_query = """
                INSERT INTO agv_positions (
                    ts, agv_id, lat, lon, heading_deg, speed_mps,
                    quality, plant_x, plant_y, zone_id, battery_percent, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            data_tuples = [
                (
                    self._parse_timestamp(d.get('ts')),
                    d.get('agv_id'),
                    d.get('lat'),
                    d.get('lon'),
                    d.get('heading_deg'),
                    d.get('speed_mps'),
                    d.get('quality'),
                    d.get('plant_x'),
                    d.get('plant_y'),
                    d.get('zone_id'),
                    d.get('battery_percent'),
                    d.get('status', 'ACTIVE')
                )
                for d in batch
            ]
            
            db_manager.execute_many(insert_query, data_tuples)
            logger.debug(f"Flushed {len(batch)} records to database")
            
        except Exception as e:
            logger.error(f"Failed to flush buffer: {e}")
            # Re-add failed items to buffer
            for item in batch:
                self.buffer.add(item, retry=True)
    
    def _parse_timestamp(self, ts_str: Any) -> datetime:
        """Parse timestamp from various formats."""
        if isinstance(ts_str, datetime):
            return ts_str
        
        if isinstance(ts_str, str):
            # Handle ISO format with or without Z
            ts_str = ts_str.replace('Z', '+00:00')
            try:
                return datetime.fromisoformat(ts_str)
            except:
                pass
        
        # Default to current time
        return datetime.now(timezone.utc)
    
    def _handle_anomaly(self, data: Dict):
        """Handle detected anomalies."""
        event_query = """
            INSERT INTO system_events (
                event_type, severity, agv_id, zone_id, message, details
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        db_manager.execute_query(
            event_query,
            (
                'ANOMALY_DETECTED',
                'WARNING',
                data.get('agv_id'),
                data.get('zone_id'),
                f"Anomaly detected for AGV {data.get('agv_id')}",
                json.dumps(data)
            )
        )
    
    def start(self):
        """Start the MQTT consumer."""
        logger.info("Starting MQTT consumer...")
        self.running = True
        
        # Setup MQTT client
        self.client = mqtt.Client(
            client_id=self.config['client_id'],
            clean_session=self.config['clean_session']
        )
        
        # Set callbacks
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        
        # Set credentials if provided
        if self.config['username'] and self.config['password']:
            self.client.username_pw_set(
                self.config['username'],
                self.config['password']
            )
        
        # Connect and start loop
        try:
            self.client.connect(
                self.config['broker'],
                self.config['port'],
                self.config['keepalive']
            )
            
            # Start periodic tasks
            self._start_periodic_tasks()
            
            # Start MQTT loop
            self.client.loop_forever()
            
        except Exception as e:
            logger.error(f"Failed to start MQTT consumer: {e}")
            raise
    
    def _start_periodic_tasks(self):
        """Start periodic maintenance tasks."""
        import threading
        
        def periodic_flush():
            while self.running:
                time.sleep(1)  # Flush every second
                if not self.buffer.is_empty():
                    self._flush_buffer()
        
        def periodic_stats():
            while self.running:
                time.sleep(60)  # Log stats every minute
                self._log_statistics()
        
        threading.Thread(target=periodic_flush, daemon=True).start()
        threading.Thread(target=periodic_stats, daemon=True).start()
    
    def _log_statistics(self):
        """Log consumer statistics."""
        with self.stats_lock:
            uptime = time.time() - self.stats['start_time']
            rate = self.stats['messages_received'] / uptime if uptime > 0 else 0
            
            logger.info(
                f"Consumer Stats - "
                f"Received: {self.stats['messages_received']}, "
                f"Processed: {self.stats['messages_processed']}, "
                f"Failed: {self.stats['messages_failed']}, "
                f"Rate: {rate:.2f} msg/s"
            )
    
    def stop(self):
        """Stop the MQTT consumer."""
        logger.info("Stopping MQTT consumer...")
        self.running = False
        
        # Flush remaining buffer
        if not self.buffer.is_empty():
            self._flush_buffer()
        
        # Disconnect MQTT
        if self.client:
            self.client.disconnect()
            self.client.loop_stop()
        
        logger.info("MQTT consumer stopped")

def main():
    """Main entry point."""
    consumer = MQTTConsumer()
    try:
        consumer.start()
    except KeyboardInterrupt:
        consumer.stop()
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        consumer.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()