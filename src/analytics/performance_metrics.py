"""
Performance metrics calculation for AGV fleet (MySQL-compatible).
"""

from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import pandas as pd
from loguru import logger

from src.core.database import db_manager


class PerformanceMetrics:
    """Calculates and tracks AGV fleet performance metrics."""

    def __init__(self):
        self.cache = {}
        self.benchmarks = self._load_benchmarks()

    def _load_benchmarks(self) -> Dict:
        """Load performance benchmarks."""
        return {
            "target_utilization": 0.85,
            "target_availability": 0.95,
            "target_task_time": 15,  # minutes
            "target_throughput": 10,  # tasks/hour
            "target_efficiency": 0.80,
            "maintenance_interval_days": 30,
        }

    def get_fleet_stats(self, time_window: timedelta = timedelta(hours=24)) -> Dict:
        """Get comprehensive fleet statistics."""
        end_time = datetime.now()
        start_time = end_time - time_window

        # Current fleet status (MySQL-safe):
        # - Replace Postgres DISTINCT ON with a max(ts) self-join
        # - Replace INTERVAL '1 minute' with INTERVAL 1 MINUTE
        # - Quote reserved identifier `type`
        fleet_status = db_manager.query_dataframe(
            """
            SELECT 
                r.agv_id,
                r.status,
                r.`type`,
                r.total_distance_km,
                r.total_runtime_hours,
                COALESCE(p.speed_mps, 0)           AS current_speed,
                COALESCE(p.battery_percent, 100)   AS battery
            FROM agv_registry r
            LEFT JOIN (
                SELECT p1.agv_id, p1.speed_mps, p1.battery_percent
                FROM agv_positions p1
                JOIN (
                    SELECT agv_id, MAX(ts) AS max_ts
                    FROM agv_positions
                    WHERE ts >= NOW() - INTERVAL 1 MINUTE
                    GROUP BY agv_id
                ) last ON last.agv_id = p1.agv_id AND last.max_ts = p1.ts
            ) p ON r.agv_id = p.agv_id
            """
        )

        # Historical metrics over window
        historical = db_manager.query_dataframe(
            """
            SELECT 
                COUNT(DISTINCT agv_id)                  AS active_agvs,
                SUM(total_distance_m) / 1000            AS total_distance_km,
                AVG(avg_speed_mps)                      AS avg_speed,
                SUM(moving_time_sec) / 3600             AS total_moving_hours,
                SUM(idle_time_sec) / 3600               AS total_idle_hours
            FROM agv_analytics_hourly
            WHERE hour_start >= %s
            """,
            (start_time,),
        )

        # Zone statistics
        zone_stats = db_manager.query_dataframe(
            """
            SELECT COUNT(DISTINCT zone_id) AS occupied_zones
            FROM agv_positions
            WHERE ts >= %s AND zone_id IS NOT NULL
            """,
            (start_time,),
        )

        total_zones_row = db_manager.execute_query(
            "SELECT COUNT(*) AS count FROM plant_zones WHERE active = TRUE"
        )
        total_zones = int(total_zones_row[0]["count"]) if total_zones_row else 0

        # Calculate top-level metrics
        total_agvs = len(fleet_status)
        active_agvs = (
            len(fleet_status[fleet_status["status"] != "OFFLINE"])
            if not fleet_status.empty
            else 0
        )

        stats_out = {
            "total_agvs": total_agvs,
            "active_agvs": active_agvs,
            "utilization": (active_agvs / total_agvs * 100) if total_agvs > 0 else 0,
            "total_distance_km": float(historical["total_distance_km"].iloc[0])
            if (not historical.empty and pd.notna(historical["total_distance_km"].iloc[0]))
            else 0.0,
            "avg_speed_mps": float(historical["avg_speed"].iloc[0])
            if (not historical.empty and pd.notna(historical["avg_speed"].iloc[0]))
            else 0.0,
            "occupied_zones": int(zone_stats["occupied_zones"].iloc[0])
            if (not zone_stats.empty and pd.notna(zone_stats["occupied_zones"].iloc[0]))
            else 0,
            "total_zones": total_zones,
            "system_health": self._calculate_system_health(fleet_status),
            "avg_battery": float(fleet_status["battery"].mean())
            if (not fleet_status.empty and pd.notna(fleet_status["battery"].mean()))
            else 100.0,
        }

        # Trend comparisons (vs. yesterday placeholder)
        yesterday_stats = self._get_yesterday_stats()
        stats_out["distance_change"] = self._calculate_change(
            stats_out["total_distance_km"], yesterday_stats.get("total_distance_km", 0)
        )
        stats_out["speed_change"] = self._calculate_change(
            stats_out["avg_speed_mps"], yesterday_stats.get("avg_speed_mps", 0)
        )
        stats_out["health_change"] = self._calculate_change(
            stats_out["system_health"], yesterday_stats.get("system_health", 100)
        )

        return stats_out

    def calculate_kpis(self, start_time: datetime, end_time: datetime) -> Dict:
        """Calculate key performance indicators."""
        # Fleet efficiency
        efficiency = self._calculate_efficiency(start_time, end_time)

        # Task metrics
        task_metrics = self._calculate_task_metrics(start_time, end_time)

        # Availability
        availability = self._calculate_availability(start_time, end_time)

        # Throughput
        throughput = self._calculate_throughput(start_time, end_time)

        kpis = {
            "efficiency": efficiency["value"],
            "efficiency_change": efficiency["change"],
            "avg_task_time": task_metrics["avg_time"],
            "task_time_change": task_metrics["change"],
            "throughput": throughput["value"],
            "throughput_change": throughput["change"],
            "availability": availability["value"],
            "availability_change": availability["change"],
            "oee": self._calculate_oee(
                efficiency["value"], availability["value"], throughput["value"]
            ),
        }
        return kpis

    def _calculate_efficiency(self, start_time: datetime, end_time: datetime) -> Dict:
        """Calculate fleet efficiency."""
        result = db_manager.query_dataframe(
            """
            SELECT 
                SUM(moving_time_sec) AS moving_time,
                SUM(idle_time_sec)   AS idle_time,
                SUM(total_distance_m) AS distance
            FROM agv_analytics_hourly
            WHERE hour_start BETWEEN %s AND %s
            """,
            (start_time, end_time),
        )

        if result.empty or pd.isna(result["moving_time"].iloc[0]):
            return {"value": 0.0, "change": 0.0}

        moving = float(result["moving_time"].iloc[0] or 0)
        idle = float(result["idle_time"].iloc[0] or 0)
        total = moving + idle

        efficiency_val = (moving / total * 100.0) if total > 0 else 0.0

        # Compare with previous period
        prev_end = start_time
        prev_start = prev_end - (end_time - start_time)
        prev_efficiency = self._calculate_efficiency(prev_start, prev_end)

        return {
            "value": efficiency_val,
            "change": efficiency_val - prev_efficiency.get("value", efficiency_val),
        }

    def _calculate_task_metrics(self, start_time: datetime, end_time: datetime) -> Dict:
        """Calculate task-related metrics."""
        result = db_manager.query_dataframe(
            """
            SELECT 
                AVG(actual_duration_sec / 60) AS avg_duration_min,
                COUNT(*)                      AS task_count,
                SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed
            FROM agv_tasks
            WHERE started_at BETWEEN %s AND %s
            """,
            (start_time, end_time),
        )

        if result.empty or pd.isna(result["avg_duration_min"].iloc[0]):
            return {"avg_time": 0.0, "change": 0.0, "completion_rate": 0.0}

        avg_time = float(result["avg_duration_min"].iloc[0] or 0)
        task_count = int(result["task_count"].iloc[0] or 0)
        completed = int(result["completed"].iloc[0] or 0)
        completion_rate = (completed / task_count * 100.0) if task_count > 0 else 0.0

        # Compare with benchmark (lower avg_time is better)
        change = (
            ((self.benchmarks["target_task_time"] - avg_time)
             / self.benchmarks["target_task_time"] * 100.0)
            if avg_time > 0
            else 0.0
        )

        return {"avg_time": avg_time, "change": change, "completion_rate": completion_rate}

    def _calculate_availability(self, start_time: datetime, end_time: datetime) -> Dict:
        """Calculate fleet availability (percent of time NOT in maintenance/charging zones)."""
        total_time = (end_time - start_time).total_seconds()

        result = db_manager.query_dataframe(
            """
            SELECT 
                agv_id,
                SUM(
                    TIMESTAMPDIFF(
                        SECOND, 
                        GREATEST(entered_at, %s),
                        LEAST(COALESCE(exited_at, NOW()), %s)
                    )
                ) AS downtime
            FROM zone_occupancy_log
            WHERE zone_id IN (
                SELECT zone_id FROM plant_zones 
                WHERE zone_type IN ('MAINTENANCE', 'CHARGING')
            )
            AND entered_at <= %s
            AND (exited_at IS NULL OR exited_at >= %s)
            GROUP BY agv_id
            """,
            (start_time, end_time, end_time, start_time),
        )

        if result.empty:
            availability = 100.0
        else:
            total_downtime = float(result["downtime"].sum() or 0)
            num_agvs_row = db_manager.execute_query(
                "SELECT COUNT(*) AS count FROM agv_registry"
            )
            num_agvs = int(num_agvs_row[0]["count"]) if num_agvs_row else 0

            total_possible_time = total_time * max(num_agvs, 0)
            availability = (
                ((total_possible_time - total_downtime) / total_possible_time * 100.0)
                if total_possible_time > 0
                else 0.0
            )

        change = availability - (self.benchmarks["target_availability"] * 100.0)
        return {"value": availability, "change": change}

    def _calculate_throughput(self, start_time: datetime, end_time: datetime) -> Dict:
        """Calculate task throughput (tasks/hour)."""
        hours = (end_time - start_time).total_seconds() / 3600.0

        result = db_manager.execute_query(
            """
            SELECT COUNT(*) AS count
            FROM agv_tasks
            WHERE completed_at BETWEEN %s AND %s
              AND status = 'COMPLETED'
            """,
            (start_time, end_time),
        )

        task_count = int(result[0]["count"]) if result else 0
        throughput = task_count / hours if hours > 0 else 0.0

        change = (
            ((throughput - self.benchmarks["target_throughput"])
             / self.benchmarks["target_throughput"] * 100.0)
            if self.benchmarks["target_throughput"] > 0
            else 0.0
        )

        return {"value": throughput, "change": change}

    def _calculate_oee(self, efficiency: float, availability: float, quality: float = 95) -> float:
        """Calculate Overall Equipment Effectiveness."""
        return (efficiency / 100.0) * (availability / 100.0) * (quality / 100.0) * 100.0

    def _calculate_system_health(self, fleet_status: pd.DataFrame) -> float:
        """Calculate overall system health score."""
        if fleet_status.empty:
            return 0.0

        factors = []

        # Battery health
        avg_battery = float(fleet_status["battery"].mean() or 0.0)
        factors.append(avg_battery / 100.0)

        # Active ratio
        active_ratio = (
            len(fleet_status[fleet_status["status"] != "OFFLINE"]) / len(fleet_status)
            if len(fleet_status) > 0
            else 0.0
        )
        factors.append(active_ratio)

        # Error rate (inverse)
        error_count = len(fleet_status[fleet_status["status"] == "ERROR"])
        error_ratio = 1.0 - (error_count / len(fleet_status)) if len(fleet_status) > 0 else 1.0
        factors.append(error_ratio)

        # Maintenance status
        # Replace Postgres INTERVAL '7 day' with MySQL syntax
        maintenance_query = """
            SELECT COUNT(*) AS due_count
            FROM agv_registry
            WHERE maintenance_due_date <= NOW() + INTERVAL 7 DAY
        """
        due_maintenance_row = db_manager.execute_query(maintenance_query)
        due_maintenance = int(due_maintenance_row[0]["due_count"]) if due_maintenance_row else 0
        maintenance_ratio = 1.0 - (due_maintenance / len(fleet_status)) if len(fleet_status) > 0 else 1.0
        factors.append(maintenance_ratio)

        # Weights: Battery, Active, Error, Maintenance
        weights = [0.25, 0.35, 0.25, 0.15]
        health = sum(f * w for f, w in zip(factors, weights)) * 100.0

        return round(health, 1)

    def get_hourly_metrics(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Get hourly performance metrics (already MySQL-compatible)."""
        return db_manager.query_dataframe(
            """
            SELECT 
                DATE_FORMAT(hour_start, '%%Y-%%m-%%d %%H:00') AS hour,
                COUNT(DISTINCT agv_id)                        AS active_agvs,
                SUM(total_distance_m) / 1000                  AS distance_km,
                AVG(avg_speed_mps)                            AS avg_speed,
                SUM(task_count)                               AS tasks,
                (SUM(moving_time_sec) / (SUM(moving_time_sec) + SUM(idle_time_sec)) * 100) AS efficiency,
                SUM(anomaly_count)                            AS anomalies
            FROM agv_analytics_hourly
            WHERE hour_start BETWEEN %s AND %s
            GROUP BY hour
            ORDER BY hour
            """,
            (start_time, end_time),
        )

    def get_utilization_by_type(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Get utilization metrics by AGV type."""
        return db_manager.query_dataframe(
            """
            SELECT 
                r.`type`                                        AS `type`,
                COUNT(DISTINCT a.agv_id)                         AS active_count,
                COUNT(DISTINCT r.agv_id)                         AS total_count,
                (COUNT(DISTINCT a.agv_id) * 100.0 / COUNT(DISTINCT r.agv_id)) AS utilization,
                AVG(a.avg_speed_mps)                             AS avg_speed,
                SUM(a.total_distance_m) / 1000                   AS total_distance_km
            FROM agv_registry r
            LEFT JOIN agv_analytics_hourly a 
                ON r.agv_id = a.agv_id
               AND a.hour_start BETWEEN %s AND %s
            GROUP BY r.`type`
            ORDER BY utilization DESC
            """,
            (start_time, end_time),
        )

    def _get_yesterday_stats(self) -> Dict:
        """Get yesterday's statistics for comparison (placeholder/demo)."""
        # For production, compute like get_fleet_stats over yesterday’s window.
        return {
            "total_distance_km": 450.0,
            "avg_speed_mps": 1.2,
            "system_health": 92.0,
        }

    def _calculate_change(self, current: float, previous: float) -> float:
        """Calculate percentage change."""
        if previous == 0:
            return 0.0
        return round((current - previous) / previous * 100.0, 1)
