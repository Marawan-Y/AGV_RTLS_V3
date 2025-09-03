"""
FastAPI application for AGV RTLS backend API.
"""

from fastapi import FastAPI, HTTPException, Depends, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import uvicorn
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import os

from loguru import logger
from src.core.database import db_manager
from src.api.websocket_handler import WebSocketManager
from src.analytics.performance_metrics import PerformanceMetrics
from src.analytics.anomaly_detector import AnomalyDetector


# Pydantic models for API
class AGVPosition(BaseModel):
    agv_id: str
    ts: datetime
    lat: float
    lon: float
    plant_x: float
    plant_y: float
    heading_deg: float
    speed_mps: float
    quality: float = 1.0
    battery_percent: float = 100.0
    status: str = "ACTIVE"


class TrajectoryRequest(BaseModel):
    agv_id: str
    start_time: datetime
    end_time: datetime
    downsample: int = 1


class ZoneStatsRequest(BaseModel):
    start_time: datetime
    end_time: datetime
    zone_ids: Optional[List[str]] = None


class FleetStatusResponse(BaseModel):
    active_agvs: int
    total_agvs: int
    utilization: float
    agvs: List[Dict[str, Any]]


class HealthCheckResponse(BaseModel):
    status: str
    timestamp: datetime
    database: str
    mqtt: str
    websocket_clients: int
    uptime_seconds: float


# Lifespan manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    # Startup
    logger.info("Starting AGV RTLS API...")
    
    # Initialize components
    app.state.ws_manager = WebSocketManager()
    app.state.performance_metrics = PerformanceMetrics()
    app.state.anomaly_detector = AnomalyDetector()
    app.state.start_time = datetime.now()
    
    # Start background tasks
    asyncio.create_task(app.state.ws_manager.broadcast_loop())
    
    yield
    
    # Shutdown
    logger.info("Shutting down AGV RTLS API...")
    await app.state.ws_manager.disconnect_all()


# Create FastAPI app
app = FastAPI(
    title="AGV RTLS API",
    description="Real-time Location System API for AGV Fleet Management",
    version="2.0.0",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# Exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "AGV RTLS API",
        "version": "2.0.0",
        "status": "operational",
        "documentation": "/docs"
    }


# Health check
@app.get("/health", response_model=HealthCheckResponse)
async def health_check(request: Request):
    """Health check endpoint."""
    
    # Check database
    try:
        db_manager.execute_query("SELECT 1")
        db_status = "healthy"
    except:
        db_status = "unhealthy"
    
    # Check MQTT (simplified check)
    mqtt_status = "healthy"  # Would check actual MQTT connection
    
    uptime = (datetime.now() - request.app.state.start_time).total_seconds()
    
    return HealthCheckResponse(
        status="healthy" if db_status == "healthy" else "degraded",
        timestamp=datetime.now(),
        database=db_status,
        mqtt=mqtt_status,
        websocket_clients=len(request.app.state.ws_manager.active_connections),
        uptime_seconds=uptime
    )


# AGV endpoints
@app.get("/api/agvs")
async def get_agvs():
    """Get list of all AGVs."""
    agvs = db_manager.execute_query("""
        SELECT 
            agv_id, display_name, type, status,
            assigned_category, last_seen
        FROM agv_registry
        ORDER BY agv_id
    """)
    return agvs


@app.get("/api/agvs/{agv_id}/position")
async def get_agv_position(agv_id: str):
    """Get current AGV position."""
    position = db_manager.execute_query("""
        SELECT 
            agv_id, ts, lat, lon, plant_x, plant_y,
            heading_deg, speed_mps, zone_id, battery_percent, status
        FROM agv_positions
        WHERE agv_id = %s
        ORDER BY ts DESC
        LIMIT 1
    """, (agv_id,))
    
    if not position:
        raise HTTPException(status_code=404, detail="AGV not found")
    
    return position[0]


@app.post("/api/agvs/{agv_id}/trajectory")
async def get_agv_trajectory(agv_id: str, request: TrajectoryRequest):
    """Get AGV trajectory for time range."""
    
    trajectory = db_manager.query_dataframe("""
        SELECT 
            ts, plant_x, plant_y, heading_deg, speed_mps, zone_id
        FROM agv_positions
        WHERE agv_id = %s AND ts BETWEEN %s AND %s
        ORDER BY ts
    """, (agv_id, request.start_time, request.end_time))
    
    # Downsample if requested
    if request.downsample > 1 and len(trajectory) > request.downsample:
        trajectory = trajectory.iloc[::request.downsample]
    
    return trajectory.to_dict('records')


@app.get("/api/fleet/status", response_model=FleetStatusResponse)
async def get_fleet_status(request: Request):
    """Get current fleet status."""
    
    stats = request.app.state.performance_metrics.get_fleet_stats()
    
    # Get AGV details
    agvs = db_manager.execute_query("""
        SELECT 
            r.agv_id,
            r.display_name,
            r.type,
            r.status,
            p.plant_x,
            p.plant_y,
            p.speed_mps,
            p.zone_id,
            p.battery_percent
        FROM agv_registry r
        LEFT JOIN (
            SELECT DISTINCT ON (agv_id)
                agv_id, plant_x, plant_y, speed_mps, zone_id, battery_percent
            FROM agv_positions
            WHERE ts >= NOW() - INTERVAL '1 minute'
            ORDER BY agv_id, ts DESC
        ) p ON r.agv_id = p.agv_id
    """)
    
    return FleetStatusResponse(
        active_agvs=stats['active_agvs'],
        total_agvs=stats['total_agvs'],
        utilization=stats['utilization'],
        agvs=agvs
    )


# Zone endpoints
@app.get("/api/zones")
async def get_zones():
    """Get all zones."""
    zones = db_manager.execute_query("""
        SELECT 
            zone_id, name, category, zone_type,
            max_agvs, max_speed_mps, priority
        FROM plant_zones
        WHERE active = TRUE
        ORDER BY zone_id
    """)
    return zones


@app.post("/api/zones/statistics")
async def get_zone_statistics(request: ZoneStatsRequest):
    """Get zone statistics for time range."""
    
    query = """
        SELECT 
            z.zone_id,
            z.name,
            z.category,
            COUNT(DISTINCT p.agv_id) as unique_agvs,
            COUNT(*) as samples,
            AVG(p.speed_mps) as avg_speed
        FROM plant_zones z
        LEFT JOIN agv_positions p ON p.zone_id = z.zone_id
            AND p.ts BETWEEN %s AND %s
        WHERE z.active = TRUE
    """
    
    params = [request.start_time, request.end_time]
    
    if request.zone_ids:
        placeholders = ','.join(['%s'] * len(request.zone_ids))
        query += f" AND z.zone_id IN ({placeholders})"
        params.extend(request.zone_ids)
    
    query += " GROUP BY z.zone_id, z.name, z.category"
    
    stats = db_manager.query_dataframe(query, tuple(params))
    return stats.to_dict('records')


@app.get("/api/zones/{zone_id}/occupancy")
async def get_zone_occupancy(zone_id: str, hours: int = 24):
    """Get zone occupancy history."""
    
    occupancy = db_manager.query_dataframe("""
        SELECT 
            DATE_FORMAT(ts, '%%Y-%%m-%%d %%H:00') as hour,
            COUNT(DISTINCT agv_id) as agv_count,
            AVG(speed_mps) as avg_speed
        FROM agv_positions
        WHERE zone_id = %s
        AND ts >= NOW() - INTERVAL %s HOUR
        GROUP BY hour
        ORDER BY hour
    """, (zone_id, hours))
    
    return occupancy.to_dict('records')


# Analytics endpoints
@app.get("/api/analytics/kpis")
async def get_kpis(request: Request, hours: int = 24):
    """Get key performance indicators."""
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    
    kpis = request.app.state.performance_metrics.calculate_kpis(start_time, end_time)
    return kpis


@app.get("/api/analytics/anomalies")
async def get_anomalies(request: Request, hours: int = 24):
    """Get recent anomalies."""
    
    stats = request.app.state.anomaly_detector.get_anomaly_statistics(
        timedelta(hours=hours)
    )
    
    # Get recent anomaly events
    events = db_manager.query_dataframe("""
        SELECT 
            event_id,
            event_type,
            severity,
            agv_id,
            zone_id,
            message,
            created_at
        FROM system_events
        WHERE created_at >= NOW() - INTERVAL %s HOUR
        AND event_type LIKE '%%ANOMALY%%'
        ORDER BY created_at DESC
        LIMIT 100
    """, (hours,))
    
    return {
        'statistics': stats,
        'events': events.to_dict('records')
    }


@app.get("/api/analytics/heatmap")
async def get_heatmap(hours: int = 24, bins: int = 150):
    """Get position heatmap data."""
    
    from src.analytics.heatmap_generator import HeatmapGenerator
    
    generator = HeatmapGenerator()
    generator.config['bins'] = bins
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    
    heatmap_data = generator.generate(start_time, end_time)
    return heatmap_data


# Task endpoints
@app.get("/api/tasks")
async def get_tasks(status: Optional[str] = None, limit: int = 100):
    """Get tasks."""
    
    query = """
        SELECT 
            task_id,
            agv_id,
            task_type,
            status,
            priority,
            origin_zone_id,
            destination_zone_id,
            created_at,
            started_at,
            completed_at
        FROM agv_tasks
    """
    
    params = []
    if status:
        query += " WHERE status = %s"
        params.append(status)
    
    query += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    
    tasks = db_manager.execute_query(query, tuple(params))
    return tasks


@app.post("/api/tasks")
async def create_task(task: Dict[str, Any]):
    """Create a new task."""
    
    # Insert task
    db_manager.execute_query("""
        INSERT INTO agv_tasks 
        (task_id, task_type, priority, origin_zone_id, destination_zone_id, status)
        VALUES (%s, %s, %s, %s, %s, 'PENDING')
    """, (
        task['task_id'],
        task['task_type'],
        task.get('priority', 5),
        task['origin_zone_id'],
        task['destination_zone_id']
    ))
    
    return {"status": "created", "task_id": task['task_id']}


# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await app.state.ws_manager.connect(websocket)


# Events endpoint
@app.get("/api/events")
async def get_events(
    severity: Optional[str] = None,
    agv_id: Optional[str] = None,
    hours: int = 24,
    limit: int = 100
):
    """Get system events."""
    
    query = """
        SELECT 
            event_id,
            event_type,
            severity,
            agv_id,
            zone_id,
            message,
            acknowledged,
            created_at
        FROM system_events
        WHERE created_at >= NOW() - INTERVAL %s HOUR
    """
    
    params = [hours]
    
    if severity:
        query += " AND severity = %s"
        params.append(severity)
    
    if agv_id:
        query += " AND agv_id = %s"
        params.append(agv_id)
    
    query += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    
    events = db_manager.execute_query(query, tuple(params))
    return events


@app.put("/api/events/{event_id}/acknowledge")
async def acknowledge_event(event_id: int, user: str = "system"):
    """Acknowledge an event."""
    
    db_manager.execute_query("""
        UPDATE system_events 
        SET acknowledged = TRUE,
            acknowledged_by = %s,
            acknowledged_at = NOW()
        WHERE event_id = %s
    """, (user, event_id))
    
    return {"status": "acknowledged", "event_id": event_id}


def run_api():
    """Run the FastAPI application."""
    uvicorn.run(
        "src.api.fastapi_app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )


if __name__ == "__main__":
    run_api()