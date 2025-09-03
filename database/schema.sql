-- Create database
CREATE DATABASE IF NOT EXISTS agv_rtls 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE agv_rtls;

-- Main positions table with partitioning support
CREATE TABLE IF NOT EXISTS agv_positions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ts DATETIME(3) NOT NULL,
    ts_received DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    agv_id VARCHAR(64) NOT NULL,
    lat DOUBLE,
    lon DOUBLE,
    heading_deg DOUBLE,
    speed_mps DOUBLE,
    acceleration_mps2 DOUBLE,
    quality DOUBLE,
    satellites INT,
    hdop DOUBLE,
    plant_x DOUBLE NOT NULL,
    plant_y DOUBLE NOT NULL,
    zone_id VARCHAR(64),
    battery_percent DOUBLE,
    payload_weight_kg DOUBLE,
    status VARCHAR(32),
    error_code VARCHAR(32),
    PRIMARY KEY (id, ts),
    INDEX idx_agv_ts (agv_id, ts DESC),
    INDEX idx_ts (ts DESC),
    INDEX idx_zone_ts (zone_id, ts DESC),
    INDEX idx_plant_coords (plant_x, plant_y),
    INDEX idx_status (status, ts DESC)
) ENGINE=InnoDB
PARTITION BY RANGE (TO_DAYS(ts)) (
    PARTITION p_history VALUES LESS THAN (TO_DAYS('2025-01-01')),
    PARTITION p_202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
    PARTITION p_202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
    PARTITION p_202503 VALUES LESS THAN (TO_DAYS('2025-04-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Zone definitions with advanced properties
CREATE TABLE IF NOT EXISTS plant_zones (
    zone_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    zone_type ENUM('RESTRICTED', 'OPERATIONAL', 'STAGING', 'MAINTENANCE', 'CHARGING') DEFAULT 'OPERATIONAL',
    max_speed_mps DOUBLE DEFAULT 2.0,
    max_agvs INT DEFAULT 5,
    priority INT DEFAULT 5,
    geom GEOMETRY NOT NULL SRID 0,
    vertices JSON,
    centroid_x DOUBLE,
    centroid_y DOUBLE,
    area_sqm DOUBLE,
    perimeter_m DOUBLE,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    SPATIAL INDEX idx_geom (geom),
    INDEX idx_category (category),
    INDEX idx_active (active)
) ENGINE=InnoDB;

-- AGV registry with extended properties
CREATE TABLE IF NOT EXISTS agv_registry (
    agv_id VARCHAR(64) PRIMARY KEY,
    display_name VARCHAR(128),
    model VARCHAR(64),
    type ENUM('TUGGER', 'FORKLIFT', 'PALLET_JACK', 'AMR', 'AGC') DEFAULT 'AGC',
    manufacturer VARCHAR(64),
    serial_number VARCHAR(128) UNIQUE,
    commissioned_date DATE,
    max_speed_mps DOUBLE DEFAULT 2.0,
    max_payload_kg DOUBLE DEFAULT 1000,
    battery_capacity_kwh DOUBLE,
    assigned_category VARCHAR(64),
    home_zone_id VARCHAR(64),
    current_task_id VARCHAR(128),
    total_distance_km DOUBLE DEFAULT 0,
    total_runtime_hours DOUBLE DEFAULT 0,
    maintenance_due_date DATE,
    status ENUM('ACTIVE', 'IDLE', 'CHARGING', 'MAINTENANCE', 'ERROR', 'OFFLINE') DEFAULT 'OFFLINE',
    last_seen DATETIME(3),
    config JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_type (type),
    FOREIGN KEY (home_zone_id) REFERENCES plant_zones(zone_id) ON DELETE SET NULL
) ENGINE=InnoDB;

-- Tasks and missions
CREATE TABLE IF NOT EXISTS agv_tasks (
    task_id VARCHAR(128) PRIMARY KEY,
    agv_id VARCHAR(64),
    task_type VARCHAR(64),
    priority INT DEFAULT 5,
    origin_zone_id VARCHAR(64),
    destination_zone_id VARCHAR(64),
    waypoints JSON,
    status ENUM('PENDING', 'ASSIGNED', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'CANCELLED') DEFAULT 'PENDING',
    assigned_at DATETIME(3),
    started_at DATETIME(3),
    completed_at DATETIME(3),
    estimated_duration_sec INT,
    actual_duration_sec INT,
    distance_m DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_agv_status (agv_id, status),
    INDEX idx_status_priority (status, priority DESC),
    FOREIGN KEY (agv_id) REFERENCES agv_registry(agv_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Analytics aggregation tables
CREATE TABLE IF NOT EXISTS agv_analytics_hourly (
    agv_id VARCHAR(64),
    hour_start DATETIME NOT NULL,
    total_distance_m DOUBLE,
    avg_speed_mps DOUBLE,
    max_speed_mps DOUBLE,
    idle_time_sec INT,
    moving_time_sec INT,
    charging_time_sec INT,
    task_count INT,
    zone_transitions INT,
    anomaly_count INT,
    PRIMARY KEY (agv_id, hour_start),
    INDEX idx_hour (hour_start DESC)
) ENGINE=InnoDB;

-- Zone occupancy tracking
CREATE TABLE IF NOT EXISTS zone_occupancy_log (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    zone_id VARCHAR(64),
    agv_id VARCHAR(64),
    entered_at DATETIME(3),
    exited_at DATETIME(3),
    duration_sec INT GENERATED ALWAYS AS (TIMESTAMPDIFF(SECOND, entered_at, exited_at)) STORED,
    INDEX idx_zone_time (zone_id, entered_at),
    INDEX idx_agv_time (agv_id, entered_at)
) ENGINE=InnoDB;

-- System events and alerts
CREATE TABLE IF NOT EXISTS system_events (
    event_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    event_type ENUM('COLLISION_RISK', 'ZONE_VIOLATION', 'SPEED_VIOLATION', 'BATTERY_LOW', 
                    'CONNECTION_LOST', 'TASK_FAILED', 'MAINTENANCE_DUE', 'ANOMALY_DETECTED') NOT NULL,
    severity ENUM('INFO', 'WARNING', 'ERROR', 'CRITICAL') DEFAULT 'INFO',
    agv_id VARCHAR(64),
    zone_id VARCHAR(64),
    message TEXT,
    details JSON,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(128),
    acknowledged_at DATETIME(3),
    created_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_type_severity (event_type, severity),
    INDEX idx_agv_created (agv_id, created_at DESC),
    INDEX idx_acknowledged (acknowledged, created_at DESC)
) ENGINE=InnoDB;

-- Calibration data
CREATE TABLE IF NOT EXISTS calibration_points (
    point_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64),
    world_x DOUBLE NOT NULL,
    world_y DOUBLE NOT NULL,
    world_crs VARCHAR(32),
    plant_x DOUBLE NOT NULL,
    plant_y DOUBLE NOT NULL,
    quality DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;