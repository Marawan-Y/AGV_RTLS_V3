"""
WebSocket handler for real-time updates.

JUSTIFICATION: While MQTT handles AGV-to-server communication, WebSocket is needed for:
1. Server-to-browser real-time updates (live dashboard updates)
2. Bi-directional communication with web clients
3. Push notifications for alerts and anomalies
4. Efficient browser updates without polling

Data flow: AGV -> MQTT -> Server -> WebSocket -> Browser Dashboard
"""

import asyncio
import json
from typing import Dict, List, Set, Any
from datetime import datetime
import weakref

from fastapi import WebSocket, WebSocketDisconnect
from loguru import logger

from src.core.database import db_manager


class ConnectionManager:
    """Manages WebSocket connections for a specific channel."""
    
    def __init__(self, channel: str):
        self.channel = channel
        self.active_connections: List[WebSocket] = []
        self.subscriptions: Dict[str, Set[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket):
        """Accept and register a new connection."""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected to channel {self.channel}")
    
    def disconnect(self, websocket: WebSocket):
        """Remove a connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        
        # Remove from subscriptions
        for topic in self.subscriptions.values():
            topic.discard(websocket)
        
        logger.info(f"WebSocket disconnected from channel {self.channel}")
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Send message to specific connection."""
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.error(f"Error sending personal message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: str, exclude: WebSocket = None):
        """Broadcast message to all connections."""
        disconnected = []
        
        for connection in self.active_connections:
            if connection != exclude:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    logger.error(f"Error broadcasting: {e}")
                    disconnected.append(connection)
        
        # Clean up disconnected
        for conn in disconnected:
            self.disconnect(conn)
    
    def subscribe(self, topic: str, websocket: WebSocket):
        """Subscribe a connection to a topic."""
        if topic not in self.subscriptions:
            self.subscriptions[topic] = set()
        self.subscriptions[topic].add(websocket)
    
    def unsubscribe(self, topic: str, websocket: WebSocket):
        """Unsubscribe a connection from a topic."""
        if topic in self.subscriptions:
            self.subscriptions[topic].discard(websocket)
    
    async def publish(self, topic: str, message: str):
        """Publish message to topic subscribers."""
        if topic in self.subscriptions:
            disconnected = []
            
            for connection in self.subscriptions[topic]:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    logger.error(f"Error publishing to topic {topic}: {e}")
                    disconnected.append(connection)
            
            # Clean up disconnected
            for conn in disconnected:
                self.disconnect(conn)


class WebSocketManager:
    """Main WebSocket manager for the application."""
    
    def __init__(self):
        self.channels: Dict[str, ConnectionManager] = {
            'positions': ConnectionManager('positions'),
            'alerts': ConnectionManager('alerts'),
            'metrics': ConnectionManager('metrics'),
            'tasks': ConnectionManager('tasks')
        }
        self.active_connections: Set[WebSocket] = set()
        self.connection_info: Dict[WebSocket, Dict] = {}
    
    async def connect(self, websocket: WebSocket):
        """Handle new WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        
        # Store connection info
        self.connection_info[websocket] = {
            'connected_at': datetime.now(),
            'subscriptions': set(),
            'client_id': None
        }
        
        # Send welcome message
        await websocket.send_json({
            'type': 'connection',
            'status': 'connected',
            'timestamp': datetime.now().isoformat(),
            'channels': list(self.channels.keys())
        })
        
        logger.info("New WebSocket connection established")
        
        # Start handling messages
        try:
            await self._handle_messages(websocket)
        except WebSocketDisconnect:
            await self.disconnect(websocket)
    
    async def disconnect(self, websocket: WebSocket):
        """Handle WebSocket disconnection."""
        self.active_connections.discard(websocket)
        
        # Remove from all channels
        for channel in self.channels.values():
            channel.disconnect(websocket)
        
        # Clean up connection info
        if websocket in self.connection_info:
            del self.connection_info[websocket]
        
        logger.info("WebSocket connection closed")
    
    async def disconnect_all(self):
        """Disconnect all WebSocket connections."""
        for websocket in list(self.active_connections):
            try:
                await websocket.close()
            except:
                pass
            await self.disconnect(websocket)
    
    async def _handle_messages(self, websocket: WebSocket):
        """Handle incoming WebSocket messages."""
        while True:
            try:
                data = await websocket.receive_json()
                await self._process_message(websocket, data)
            except WebSocketDisconnect:
                break
            except json.JSONDecodeError:
                await websocket.send_json({
                    'type': 'error',
                    'message': 'Invalid JSON'
                })
            except Exception as e:
                logger.error(f"Error handling WebSocket message: {e}")
                await websocket.send_json({
                    'type': 'error',
                    'message': str(e)
                })
    
    async def _process_message(self, websocket: WebSocket, data: Dict):
        """Process incoming WebSocket message."""
        msg_type = data.get('type')
        
        if msg_type == 'subscribe':
            # Subscribe to channel
            channel = data.get('channel')
            if channel in self.channels:
                await self.channels[channel].connect(websocket)
                self.connection_info[websocket]['subscriptions'].add(channel)
                
                await websocket.send_json({
                    'type': 'subscribed',
                    'channel': channel
                })
            else:
                await websocket.send_json({
                    'type': 'error',
                    'message': f'Unknown channel: {channel}'
                })
        
        elif msg_type == 'unsubscribe':
            # Unsubscribe from channel
            channel = data.get('channel')
            if channel in self.channels:
                self.channels[channel].disconnect(websocket)
                self.connection_info[websocket]['subscriptions'].discard(channel)
                
                await websocket.send_json({
                    'type': 'unsubscribed',
                    'channel': channel
                })
        
        elif msg_type == 'ping':
            # Respond to ping
            await websocket.send_json({
                'type': 'pong',
                'timestamp': datetime.now().isoformat()
            })
        
        elif msg_type == 'identify':
            # Store client identification
            client_id = data.get('client_id')
            self.connection_info[websocket]['client_id'] = client_id
            
            await websocket.send_json({
                'type': 'identified',
                'client_id': client_id
            })
    
    async def broadcast_position_update(self, position_data: Dict):
        """Broadcast AGV position update."""
        message = json.dumps({
            'type': 'position_update',
            'data': position_data,
            'timestamp': datetime.now().isoformat()
        })
        
        await self.channels['positions'].broadcast(message)
    
    async def broadcast_alert(self, alert_data: Dict):
        """Broadcast alert/anomaly."""
        message = json.dumps({
            'type': 'alert',
            'data': alert_data,
            'timestamp': datetime.now().isoformat()
        })
        
        await self.channels['alerts'].broadcast(message)
    
    async def broadcast_metrics(self, metrics_data: Dict):
        """Broadcast performance metrics."""
        message = json.dumps({
            'type': 'metrics_update',
            'data': metrics_data,
            'timestamp': datetime.now().isoformat()
        })
        
        await self.channels['metrics'].broadcast(message)
    
    async def broadcast_loop(self):
        """Main broadcast loop for real-time updates."""
        logger.info("Starting WebSocket broadcast loop")
        
        while True:
            try:
                # Broadcast position updates every second
                positions = db_manager.query_dataframe("""
                    SELECT 
                        agv_id, plant_x, plant_y, heading_deg, 
                        speed_mps, zone_id, battery_percent
                    FROM agv_positions
                    WHERE ts >= NOW() - INTERVAL 5 SECOND
                    ORDER BY agv_id, ts DESC
                """)
                
                if not positions.empty:
                    # Get latest position per AGV
                    latest = positions.drop_duplicates(subset=['agv_id'], keep='first')
                    await self.broadcast_position_update(latest.to_dict('records'))
                
                # Check for new alerts every 5 seconds
                if asyncio.get_event_loop().time() % 5 < 1:
                    alerts = db_manager.execute_query("""
                        SELECT 
                            event_id, event_type, severity, 
                            agv_id, message
                        FROM system_events
                        WHERE created_at >= NOW() - INTERVAL 5 SECOND
                        AND severity IN ('WARNING', 'ERROR', 'CRITICAL')
                    """)
                    
                    if alerts:
                        for alert in alerts:
                            await self.broadcast_alert(alert)
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in broadcast loop: {e}")
                await asyncio.sleep(5)