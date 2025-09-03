-- Advanced Indexes for AGV RTLS Database
-- Optimized for high-frequency queries

USE agv_rtls;

-- Composite indexes for time-based queries
CREATE INDEX idx_positions_agv_ts_zone 
ON agv_positions(agv_id, ts DESC, zone_id)
COMMENT 'Optimized for AGV trajectory with zone info';

CREATE INDEX idx_positions_ts_status 
ON agv_positions(ts DESC, status)
COMMENT 'For fleet status queries';

CREATE INDEX idx_positions_zone_ts_agv 
ON agv_positions(zone_id, ts DESC, agv_id)
COMMENT 'For zone occupancy queries';

-- Spatial index for location queries
ALTER TABLE agv_positions 
ADD COLUMN location POINT 
GENERATED ALWAYS AS (POINT(plant_x, plant_y)) STORED;

CREATE SPATIAL INDEX idx_positions_location 
ON agv_positions(location);

-- Covering indexes for common queries
CREATE INDEX idx_positions_covering_trajectory
ON agv_positions(agv_id, ts, plant_x, plant_y, heading_deg, speed_mps)
COMMENT 'Covering index for trajectory queries';

-- Indexes for analytics queries
CREATE INDEX idx_analytics_hourly_lookup
ON agv_analytics_hourly(hour_start DESC, agv_id)
COMMENT 'For time-series analytics';

CREATE INDEX idx_analytics_hourly_agv_lookup
ON agv_analytics_hourly(agv_id, hour_start DESC)
COMMENT 'For per-AGV analytics';

-- Zone occupancy indexes
CREATE INDEX idx_zone_occupancy_active
ON zone_occupancy_log(zone_id, exited_at)
WHERE exited_at IS NULL
COMMENT 'For current zone occupancy';

CREATE INDEX idx_zone_occupancy_history
ON zone_occupancy_log(zone_id, entered_at DESC)
COMMENT 'For zone history queries';

-- Task management indexes
CREATE INDEX idx_tasks_active
ON agv_tasks(status, priority DESC, created_at)
WHERE status IN ('PENDING', 'ASSIGNED', 'IN_PROGRESS')
COMMENT 'For active task queries';

CREATE INDEX idx_tasks_agv_active
ON agv_tasks(agv_id, status, started_at DESC)
WHERE status = 'IN_PROGRESS'
COMMENT 'For current AGV tasks';

-- Event system indexes
CREATE INDEX idx_events_recent_critical
ON system_events(created_at DESC, severity)
WHERE severity IN ('ERROR', 'CRITICAL') AND acknowledged = FALSE
COMMENT 'For critical alert queries';

CREATE INDEX idx_events_agv_history
ON system_events(agv_id, created_at DESC, event_type)
COMMENT 'For AGV event history';

-- Registry indexes
CREATE INDEX idx_registry_active_status
ON agv_registry(status, last_seen DESC)
WHERE status != 'OFFLINE'
COMMENT 'For active AGV queries';

CREATE INDEX idx_registry_maintenance
ON agv_registry(maintenance_due_date, agv_id)
WHERE maintenance_due_date IS NOT NULL
COMMENT 'For maintenance scheduling';

-- Full-text search indexes
CREATE FULLTEXT INDEX idx_events_message_search
ON system_events(message);

CREATE FULLTEXT INDEX idx_tasks_search
ON agv_tasks(task_type);

-- Performance optimization indexes
CREATE INDEX idx_positions_partition_helper
ON agv_positions(ts, id)
COMMENT 'Helps with partition pruning';

-- Statistics update schedule
CREATE EVENT IF NOT EXISTS update_index_statistics
ON SCHEDULE EVERY 1 HOUR
DO
  ANALYZE TABLE agv_positions;
  ANALYZE TABLE agv_analytics_hourly;
  ANALYZE TABLE zone_occupancy_log;
  ANALYZE TABLE agv_tasks;