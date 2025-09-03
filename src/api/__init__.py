"""API routes module."""

from fastapi import APIRouter

# Create routers for different endpoints
agv_router = APIRouter(prefix="/agvs", tags=["AGVs"])
zone_router = APIRouter(prefix="/zones", tags=["Zones"])
analytics_router = APIRouter(prefix="/analytics", tags=["Analytics"])
task_router = APIRouter(prefix="/tasks", tags=["Tasks"])
system_router = APIRouter(prefix="/system", tags=["System"])

__all__ = [
    'agv_router',
    'zone_router',
    'analytics_router',
    'task_router',
    'system_router'
]