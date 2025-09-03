"""
Enhanced MQTT client for AGV RTLS system.
"""

import os
import json
import time
import threading
from typing import Dict, Any, Callable, Optional
from datetime import datetime
from queue import Queue, Empty
import paho.mqtt.client as mqtt

from loguru import logger
from src.core.metrics import metrics_collector


class MQTTClient:
    """Enhanced MQTT client with reconnection and buffering."""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
        self.client = None
        self.connected = False
        self.message_queue = Queue(maxsize=10000)
        self.callbacks = {}
        self.reconnect_timer = None
        self.stats = {
            'messages_received': 0,
            'messages_sent': 0,
            'connection_attempts': 0,
            'last_connected': None
        }
    
    def _default_config(self) -> Dict:
        """Get default MQTT configuration."""
        return {
            'broker': os.getenv('MQTT_BROKER', 'localhost'),
            'port': int(os.getenv('MQTT_PORT', 1883)),
            'username': os.getenv('MQTT_USERNAME'),
            'password': os.getenv('MQTT_PASSWORD'),
            'client_id': f"agv_client_{os.getpid()}",
            'keepalive': 60,
            'qos': 1,
            'clean_session': False,
            'reconnect_delay': 5,
            'max_reconnect_delay': 60,
            'topics': []
        }
    
    def on_connect(self, client, userdata, flags, rc):
        """Handle connection event."""
        if rc == 0:
            self.connected = True
            self.stats['last_connected'] = datetime.now()
            logger.info(f"Connected to MQTT broker at {self.config['broker']}")
            
            # Resubscribe to topics
            for topic in self.config['topics']:
                client.subscribe(topic, qos=self.config['qos'])
                logger.info(f"Subscribed to {topic}")
            
            # Process queued messages
            self._process_queue()
            
            metrics_collector.record_message('mqtt', 'connection', 0)
        else:
            logger.error(f"Connection failed with code {rc}")
            self.connected = False
            self._schedule_reconnect()
    
    def on_disconnect(self, client, userdata, rc):
        """Handle disconnection event."""
        self.connected = False
        logger.warning(f"Disconnected from MQTT broker (code: {rc})")
        
        if rc != 0:  # Unexpected disconnection
            self._schedule_reconnect()
        
        metrics_collector.record_error('mqtt_disconnect', 'mqtt_client')
    
    def on_message(self, client, userdata, msg):
        """Handle incoming message."""
        try:
            self.stats['messages_received'] += 1
            
            # Parse message
            payload = json.loads(msg.payload.decode('utf-8'))
            
            # Record metrics
            agv_id = payload.get('agv_id', 'unknown')
            metrics_collector.record_message('mqtt', agv_id)
            
            # Call registered callbacks
            for pattern, callback in self.callbacks.items():
                if self._topic_matches(pattern, msg.topic):
                    callback(msg.topic, payload)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            metrics_collector.record_error('json_decode', 'mqtt_client')
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            metrics_collector.record_error('message_processing', 'mqtt_client')
    
    def connect(self) -> bool:
        """Connect to MQTT broker."""
        try:
            self.stats['connection_attempts'] += 1
            
            # Create client
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
            
            # Connect
            self.client.connect(
                self.config['broker'],
                self.config['port'],
                self.config['keepalive']
            )
            
            # Start loop
            self.client.loop_start()
            
            # Wait for connection
            timeout = 10
            while not self.connected and timeout > 0:
                time.sleep(0.1)
                timeout -= 0.1
            
            return self.connected
            
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            metrics_collector.record_error('connection_failed', 'mqtt_client')
            return False
    
    def disconnect(self):
        """Disconnect from MQTT broker."""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False
            logger.info("Disconnected from MQTT broker")
    
    def subscribe(self, topic: str, callback: Callable = None):
        """Subscribe to a topic."""
        self.config['topics'].append(topic)
        
        if callback:
            self.callbacks[topic] = callback
        
        if self.connected and self.client:
            self.client.subscribe(topic, qos=self.config['qos'])
            logger.info(f"Subscribed to {topic}")
    
    def unsubscribe(self, topic: str):
        """Unsubscribe from a topic."""
        if topic in self.config['topics']:
            self.config['topics'].remove(topic)
        
        if topic in self.callbacks:
            del self.callbacks[topic]
        
        if self.connected and self.client:
            self.client.unsubscribe(topic)
            logger.info(f"Unsubscribed from {topic}")
    
    def publish(self, topic: str, payload: Dict, qos: int = None, retain: bool = False):
        """Publish a message."""
        if qos is None:
            qos = self.config['qos']
        
        message = json.dumps(payload)
        
        if self.connected and self.client:
            try:
                result = self.client.publish(topic, message, qos=qos, retain=retain)
                
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    self.stats['messages_sent'] += 1
                    return True
                else:
                    logger.error(f"Failed to publish message: {result.rc}")
                    self._queue_message(topic, payload, qos, retain)
                    return False
                    
            except Exception as e:
                logger.error(f"Error publishing message: {e}")
                self._queue_message(topic, payload, qos, retain)
                return False
        else:
            self._queue_message(topic, payload, qos, retain)
            return False
    
    def _queue_message(self, topic: str, payload: Dict, qos: int, retain: bool):
        """Queue message for later delivery."""
        try:
            self.message_queue.put_nowait({
                'topic': topic,
                'payload': payload,
                'qos': qos,
                'retain': retain,
                'timestamp': datetime.now()
            })
            logger.debug(f"Queued message for topic {topic}")
        except:
            logger.warning("Message queue full, dropping message")
            metrics_collector.record_error('queue_full', 'mqtt_client')
    
    def _process_queue(self):
        """Process queued messages."""
        processed = 0
        
        while not self.message_queue.empty() and self.connected:
            try:
                msg = self.message_queue.get_nowait()
                
                # Check message age
                age = (datetime.now() - msg['timestamp']).seconds
                if age < 300:  # Discard messages older than 5 minutes
                    self.publish(
                        msg['topic'],
                        msg['payload'],
                        msg['qos'],
                        msg['retain']
                    )
                    processed += 1
                    
            except Empty:
                break
            except Exception as e:
                logger.error(f"Error processing queued message: {e}")
        
        if processed > 0:
            logger.info(f"Processed {processed} queued messages")
    
    def _schedule_reconnect(self):
        """Schedule reconnection attempt."""
        if self.reconnect_timer:
            self.reconnect_timer.cancel()
        
        delay = min(
            self.config['reconnect_delay'] * (2 ** min(self.stats['connection_attempts'], 5)),
            self.config['max_reconnect_delay']
        )
        
        logger.info(f"Scheduling reconnection in {delay} seconds")
        
        self.reconnect_timer = threading.Timer(delay, self._reconnect)
        self.reconnect_timer.start()
    
    def _reconnect(self):
        """Attempt to reconnect."""
        logger.info("Attempting to reconnect to MQTT broker")
        self.connect()
    
    def _topic_matches(self, pattern: str, topic: str) -> bool:
        """Check if topic matches pattern (supports wildcards)."""
        pattern_parts = pattern.split('/')
        topic_parts = topic.split('/')
        
        if '+' not in pattern and '#' not in pattern:
            return pattern == topic
        
        for i, (p, t) in enumerate(zip(pattern_parts, topic_parts)):
            if p == '#':
                return True
            elif p == '+':
                continue
            elif p != t:
                return False
        
        return len(pattern_parts) == len(topic_parts)
    
    def get_stats(self) -> Dict:
        """Get client statistics."""
        return {
            **self.stats,
            'connected': self.connected,
            'queued_messages': self.message_queue.qsize()
        }