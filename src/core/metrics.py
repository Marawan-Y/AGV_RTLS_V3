"""
Metrics collection and monitoring for the AGV RTLS system.
"""

import time
import psutil
import threading
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from collections import deque, defaultdict
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from loguru import logger


class MetricsCollector:
    """Collects and exposes system metrics for monitoring."""
    
    def __init__(self):
        self._init_prometheus_metrics()
        self._init_internal_metrics()
        self._start_collection_thread()
    
    def _init_prometheus_metrics(self):
        """Initialize Prometheus metrics."""
        
        # Counters
        self.messages_received = Counter(
            'agv_messages_received_total',
            'Total number of messages received',
            ['source', 'agv_id']
        )
        
        self.messages_processed = Counter(
            'agv_messages_processed_total',
            'Total number of messages processed successfully',
            ['agv_id']
        )
        
        self.errors = Counter(
            'agv_errors_total',
            'Total number of errors',
            ['type', 'component']
        )
        
        self.database_queries = Counter(
            'agv_database_queries_total',
            'Total number of database queries',
            ['query_type']
        )
        
        # Gauges
        self.active_agvs = Gauge(
            'agv_active_count',
            'Number of active AGVs'
        )
        
        self.fleet_utilization = Gauge(
            'agv_fleet_utilization_percent',
            'Fleet utilization percentage'
        )
        
        self.websocket_connections = Gauge(
            'agv_websocket_connections',
            'Number of active WebSocket connections'
        )
        
        self.database_connections = Gauge(
            'agv_database_connections',
            'Number of active database connections'
        )
        
        self.system_cpu_percent = Gauge(
            'agv_system_cpu_percent',
            'System CPU usage percentage'
        )
        
        self.system_memory_percent = Gauge(
            'agv_system_memory_percent',
            'System memory usage percentage'
        )
        
        # Histograms
        self.message_processing_time = Histogram(
            'agv_message_processing_seconds',
            'Message processing time in seconds',
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        )
        
        self.database_query_time = Histogram(
            'agv_database_query_seconds',
            'Database query execution time',
            ['query_type'],
            buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        
        self.api_request_time = Histogram(
            'agv_api_request_seconds',
            'API request processing time',
            ['endpoint', 'method'],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        )
    
    def _init_internal_metrics(self):
        """Initialize internal metrics storage."""
        self.metrics_buffer = deque(maxlen=10000)
        self.rate_counters = defaultdict(lambda: deque(maxlen=300))  # 5 minutes at 1Hz
        self.performance_stats = {}
        self.start_time = time.time()
    
    def _start_collection_thread(self):
        """Start background metrics collection thread."""
        self.collection_thread = threading.Thread(
            target=self._collect_system_metrics,
            daemon=True
        )
        self.collection_thread.start()
    
    def _collect_system_metrics(self):
        """Collect system metrics periodically."""
        while True:
            try:
                # CPU and memory
                self.system_cpu_percent.set(psutil.cpu_percent(interval=1))
                self.system_memory_percent.set(psutil.virtual_memory().percent)
                
                # Database connections (simplified)
                from src.core.database import db_manager
                try:
                    result = db_manager.execute_query("""
                        SELECT COUNT(*) as count 
                        FROM information_schema.processlist 
                        WHERE db = DATABASE()
                    """)
                    self.database_connections.set(result[0]['count'])
                except:
                    pass
                
                # Active AGVs
                try:
                    result = db_manager.execute_query("""
                        SELECT COUNT(DISTINCT agv_id) as count
                        FROM agv_positions
                        WHERE ts >= NOW() - INTERVAL 1 MINUTE
                    """)
                    self.active_agvs.set(result[0]['count'])
                except:
                    pass
                
                time.sleep(10)  # Collect every 10 seconds
                
            except Exception as e:
                logger.error(f"Error collecting system metrics: {e}")
                time.sleep(30)
    
    def record_message(self, source: str, agv_id: str, processing_time: float = None):
        """Record message metrics."""
        self.messages_received.labels(source=source, agv_id=agv_id).inc()
        
        if processing_time:
            self.message_processing_time.observe(processing_time)
            self.messages_processed.labels(agv_id=agv_id).inc()
        
        # Update rate counter
        self.rate_counters[f'messages_{agv_id}'].append(time.time())
    
    def record_error(self, error_type: str, component: str):
        """Record error metrics."""
        self.errors.labels(type=error_type, component=component).inc()
        
        # Log to buffer
        self.metrics_buffer.append({
            'type': 'error',
            'error_type': error_type,
            'component': component,
            'timestamp': datetime.now()
        })
    
    def record_database_query(self, query_type: str, execution_time: float):
        """Record database query metrics."""
        self.database_queries.labels(query_type=query_type).inc()
        self.database_query_time.labels(query_type=query_type).observe(execution_time)
    
    def record_api_request(self, endpoint: str, method: str, response_time: float):
        """Record API request metrics."""
        self.api_request_time.labels(
            endpoint=endpoint,
            method=method
        ).observe(response_time)
    
    def get_message_rate(self, agv_id: str = None) -> float:
        """Calculate message rate (messages per second)."""
        if agv_id:
            timestamps = self.rate_counters[f'messages_{agv_id}']
        else:
            # Aggregate all AGVs
            timestamps = []
            for key, values in self.rate_counters.items():
                if key.startswith('messages_'):
                    timestamps.extend(values)
        
        if len(timestamps) < 2:
            return 0.0
        
        # Calculate rate over last 60 seconds
        current_time = time.time()
        recent = [t for t in timestamps if current_time - t <= 60]
        
        if len(recent) < 2:
            return 0.0
        
        duration = recent[-1] - recent[0]
        return len(recent) / duration if duration > 0 else 0.0
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health metrics."""
        uptime = time.time() - self.start_time
        
        # Calculate error rate
        recent_errors = [
            m for m in self.metrics_buffer
            if m.get('type') == 'error' and
            (datetime.now() - m['timestamp']).seconds < 300
        ]
        
        health = {
            'status': 'healthy',
            'uptime_seconds': uptime,
            'uptime_hours': uptime / 3600,
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent,
            'message_rate': self.get_message_rate(),
            'error_rate': len(recent_errors) / 300,  # Errors per second
            'timestamp': datetime.now().isoformat()
        }
        
        # Determine health status
        if health['cpu_percent'] > 90 or health['memory_percent'] > 90:
            health['status'] = 'degraded'
        elif health['error_rate'] > 1.0:
            health['status'] = 'unhealthy'
        
        return health
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary statistics."""
        return {
            'total_messages': sum(
                self.messages_received._metrics.values()
            ) if hasattr(self.messages_received, '_metrics') else 0,
            'total_errors': sum(
                self.errors._metrics.values()
            ) if hasattr(self.errors, '_metrics') else 0,
            'uptime_hours': (time.time() - self.start_time) / 3600,
            'current_message_rate': self.get_message_rate(),
            'system_health': self.get_system_health()
        }
    
    def export_prometheus(self) -> bytes:
        """Export metrics in Prometheus format."""
        return generate_latest()
    
    def reset_metrics(self):
        """Reset all metrics (use with caution)."""
        self._init_prometheus_metrics()
        self._init_internal_metrics()
        logger.warning("All metrics have been reset")


# Global metrics collector instance
metrics_collector = MetricsCollector()