-- Stored Procedures for AGV RTLS Database
USE agv_rtls;

DELIMITER $$

-- Get AGV trajectory with downsampling
CREATE PROCEDURE GetAGVTrajectory(
    IN p_agv_id VARCHAR(64),
    IN p_start_time DATETIME,
    IN p_end_time DATETIME,
    IN p_downsample INT
)
BEGIN
    SET @row_num = 0;
    
    SELECT * FROM (
        SELECT 
            @row_num := @row_num + 1 AS row_num,
            ts, plant_x, plant_y, heading_deg, speed_mps, zone_id
        FROM agv_positions
        WHERE agv_id = p_agv_id 
        AND ts BETWEEN p_start_time AND p_end_time
        ORDER BY ts
    ) AS numbered_rows
    WHERE MOD(row_num - 1, p_downsample) = 0;
END$$

-- Calculate zone dwell times
CREATE PROCEDURE CalculateZoneDwellTime(
    IN p_start_time DATETIME,
    IN p_end_time DATETIME
)
BEGIN
    SELECT 
        z.zone_id,
        z.name AS zone_name,
        z.category,
        COUNT(DISTINCT p.agv_id) AS unique_agvs,
        COUNT(*) AS total_samples,
        ROUND(COUNT(*) / 3.0 / 60, 2) AS total_minutes,
        ROUND(COUNT(*) / COUNT(DISTINCT p.agv_id) / 3.0 / 60, 2) AS avg_minutes_per_agv
    FROM agv_positions p
    INNER JOIN plant_zones z ON p.zone_id = z.zone_id
    WHERE p.ts BETWEEN p_start_time AND p_end_time
    GROUP BY z.zone_id, z.name, z.category
    ORDER BY total_minutes DESC;
END$$

-- Get current fleet status
CREATE PROCEDURE GetFleetStatus()
BEGIN
    SELECT 
        r.agv_id,
        r.display_name,
        r.type,
        r.status,
        r.battery_percent,
        COALESCE(p.zone_id, r.home_zone_id) AS current_zone,
        z.name AS zone_name,
        TIMESTAMPDIFF(SECOND, r.last_seen, NOW()) AS seconds_since_update,
        t.task_id AS current_task,
        t.destination_zone_id,
        p.speed_mps,
        p.heading_deg
    FROM agv_registry r
    LEFT JOIN (
        SELECT p1.* FROM agv_positions p1
        INNER JOIN (
            SELECT agv_id, MAX(ts) AS max_ts
            FROM agv_positions
            WHERE ts >= NOW() - INTERVAL 1 MINUTE
            GROUP BY agv_id
        ) p2 ON p1.agv_id = p2.agv_id AND p1.ts = p2.max_ts
    ) p ON r.agv_id = p.agv_id
    LEFT JOIN plant_zones z ON COALESCE(p.zone_id, r.home_zone_id) = z.zone_id
    LEFT JOIN agv_tasks t ON r.agv_id = t.agv_id AND t.status = 'IN_PROGRESS'
    ORDER BY r.agv_id;
END$$

-- Calculate AGV performance metrics
CREATE PROCEDURE CalculateAGVMetrics(
    IN p_agv_id VARCHAR(64),
    IN p_date DATE
)
BEGIN
    DECLARE v_total_distance DOUBLE DEFAULT 0;
    DECLARE v_total_time INT DEFAULT 0;
    DECLARE v_idle_time INT DEFAULT 0;
    DECLARE v_moving_time INT DEFAULT 0;
    DECLARE v_avg_speed DOUBLE DEFAULT 0;
    DECLARE v_max_speed DOUBLE DEFAULT 0;
    DECLARE v_task_count INT DEFAULT 0;
    
    -- Calculate distance traveled
    SELECT 
        COALESCE(SUM(
            SQRT(
                POW(plant_x - LAG(plant_x) OVER (ORDER BY ts), 2) + 
                POW(plant_y - LAG(plant_y) OVER (ORDER BY ts), 2)
            )
        ), 0) INTO v_total_distance
    FROM agv_positions
    WHERE agv_id = p_agv_id
    AND DATE(ts) = p_date;
    
    -- Calculate time metrics
    SELECT 
        COUNT(*) / 3,  -- Total time in seconds (3Hz sampling)
        SUM(CASE WHEN speed_mps < 0.1 THEN 1 ELSE 0 END) / 3,  -- Idle time
        SUM(CASE WHEN speed_mps >= 0.1 THEN 1 ELSE 0 END) / 3,  -- Moving time
        AVG(CASE WHEN speed_mps >= 0.1 THEN speed_mps ELSE NULL END),
        MAX(speed_mps)
    INTO v_total_time, v_idle_time, v_moving_time, v_avg_speed, v_max_speed
    FROM agv_positions
    WHERE agv_id = p_agv_id
    AND DATE(ts) = p_date;
    
    -- Count completed tasks
    SELECT COUNT(*) INTO v_task_count
    FROM agv_tasks
    WHERE agv_id = p_agv_id
    AND DATE(completed_at) = p_date
    AND status = 'COMPLETED';
    
    -- Return metrics
    SELECT 
        p_agv_id AS agv_id,
        p_date AS date,
        ROUND(v_total_distance, 2) AS total_distance_m,
        v_total_time AS total_time_sec,
        v_idle_time AS idle_time_sec,
        v_moving_time AS moving_time_sec,
        ROUND(v_idle_time * 100.0 / NULLIF(v_total_time, 0), 2) AS idle_percentage,
        ROUND(v_avg_speed, 2) AS avg_speed_mps,
        ROUND(v_max_speed, 2) AS max_speed_mps,
        v_task_count AS tasks_completed,
        ROUND(v_total_distance / NULLIF(v_task_count, 0), 2) AS avg_distance_per_task;
END$$

-- Detect zone violations
CREATE PROCEDURE DetectZoneViolations(
    IN p_check_minutes INT
)
BEGIN
    -- Check for AGVs in restricted zones
    INSERT INTO system_events (
        event_type, severity, agv_id, zone_id, message, details
    )
    SELECT 
        'ZONE_VIOLATION',
        'WARNING',
        p.agv_id,
        p.zone_id,
        CONCAT('AGV ', p.agv_id, ' entered restricted zone ', z.name),
        JSON_OBJECT(
            'zone_type', z.zone_type,
            'timestamp', p.ts,
            'position', JSON_ARRAY(p.plant_x, p.plant_y)
        )
    FROM agv_positions p
    INNER JOIN plant_zones z ON p.zone_id = z.zone_id
    WHERE z.zone_type = 'RESTRICTED'
    AND p.ts >= NOW() - INTERVAL p_check_minutes MINUTE
    AND NOT EXISTS (
        SELECT 1 FROM system_events e
        WHERE e.event_type = 'ZONE_VIOLATION'
        AND e.agv_id = p.agv_id
        AND e.zone_id = p.zone_id
        AND e.created_at >= NOW() - INTERVAL 5 MINUTE
    );
    
    -- Check for overcrowded zones
    INSERT INTO system_events (
        event_type, severity, agv_id, zone_id, message, details
    )
    SELECT 
        'ZONE_VIOLATION',
        'WARNING',
        NULL,
        zone_id,
        CONCAT('Zone ', zone_name, ' exceeded maximum AGV capacity'),
        JSON_OBJECT(
            'current_agvs', agv_count,
            'max_allowed', max_agvs,
            'agv_list', agv_list
        )
    FROM (
        SELECT 
            p.zone_id,
            z.name AS zone_name,
            z.max_agvs,
            COUNT(DISTINCT p.agv_id) AS agv_count,
            GROUP_CONCAT(DISTINCT p.agv_id) AS agv_list
        FROM agv_positions p
        INNER JOIN plant_zones z ON p.zone_id = z.zone_id
        WHERE p.ts >= NOW() - INTERVAL 10 SECOND
        GROUP BY p.zone_id, z.name, z.max_agvs
        HAVING agv_count > z.max_agvs
    ) AS overcrowded;
END$$

-- Archive old data
CREATE PROCEDURE ArchiveOldData(
    IN p_days_to_keep INT
)
BEGIN
    DECLARE v_cutoff_date DATETIME;
    DECLARE v_archived_count INT;
    DECLARE v_deleted_count INT;
    
    SET v_cutoff_date = NOW() - INTERVAL p_days_to_keep DAY;
    
    START TRANSACTION;
    
    -- Create archive table if not exists
    CREATE TABLE IF NOT EXISTS agv_positions_archive LIKE agv_positions;
    
    -- Archive data
    INSERT INTO agv_positions_archive
    SELECT * FROM agv_positions
    WHERE ts < v_cutoff_date;
    
    SET v_archived_count = ROW_COUNT();
    
    -- Delete archived data
    DELETE FROM agv_positions
    WHERE ts < v_cutoff_date
    LIMIT 100000;
    
    SET v_deleted_count = ROW_COUNT();
    
    -- Log the operation
    INSERT INTO system_events (
        event_type, severity, message, details
    ) VALUES (
        'MAINTENANCE',
        'INFO',
        'Data archival completed',
        JSON_OBJECT(
            'archived_records', v_archived_count,
            'deleted_records', v_deleted_count,
            'cutoff_date', v_cutoff_date
        )
    );
    
    COMMIT;
    
    SELECT v_archived_count AS archived, v_deleted_count AS deleted;
END$$

-- Generate zone transition matrix
CREATE PROCEDURE GenerateZoneTransitionMatrix(
    IN p_start_time DATETIME,
    IN p_end_time DATETIME
)
BEGIN
    WITH transitions AS (
        SELECT 
            agv_id,
            zone_id AS from_zone,
            LEAD(zone_id) OVER (PARTITION BY agv_id ORDER BY ts) AS to_zone,
            ts AS transition_time
        FROM agv_positions
        WHERE ts BETWEEN p_start_time AND p_end_time
        AND zone_id IS NOT NULL
    )
    SELECT 
        from_zone,
        to_zone,
        COUNT(*) AS transition_count,
        COUNT(DISTINCT agv_id) AS unique_agvs
    FROM transitions
    WHERE to_zone IS NOT NULL
    AND from_zone != to_zone
    GROUP BY from_zone, to_zone
    ORDER BY transition_count DESC;
END$$

-- Calculate fleet KPIs
CREATE PROCEDURE CalculateFleetKPIs(
    IN p_date DATE
)
BEGIN
    DECLARE v_total_agvs INT;
    DECLARE v_active_agvs INT;
    DECLARE v_total_distance DOUBLE;
    DECLARE v_total_tasks INT;
    DECLARE v_avg_task_time DOUBLE;
    DECLARE v_utilization DOUBLE;
    
    -- Get AGV counts
    SELECT COUNT(*) INTO v_total_agvs FROM agv_registry;
    
    SELECT COUNT(DISTINCT agv_id) INTO v_active_agvs
    FROM agv_positions
    WHERE DATE(ts) = p_date;
    
    -- Calculate total distance
    SELECT COALESCE(SUM(total_distance_m), 0) INTO v_total_distance
    FROM agv_analytics_hourly
    WHERE DATE(hour_start) = p_date;
    
    -- Calculate task metrics
    SELECT 
        COUNT(*),
        AVG(actual_duration_sec / 60.0)
    INTO v_total_tasks, v_avg_task_time
    FROM agv_tasks
    WHERE DATE(completed_at) = p_date
    AND status = 'COMPLETED';
    
    -- Calculate utilization
    SET v_utilization = (v_active_agvs * 100.0) / NULLIF(v_total_agvs, 0);
    
    SELECT 
        p_date AS date,
        v_total_agvs AS total_agvs,
        v_active_agvs AS active_agvs,
        ROUND(v_utilization, 2) AS fleet_utilization_percent,
        ROUND(v_total_distance / 1000, 2) AS total_distance_km,
        v_total_tasks AS tasks_completed,
        ROUND(v_avg_task_time, 2) AS avg_task_time_minutes,
        ROUND(v_total_tasks / 24.0, 2) AS tasks_per_hour;
END$$

DELIMITER ;

-- Create scheduled events
CREATE EVENT IF NOT EXISTS hourly_analytics
ON SCHEDULE EVERY 1 HOUR
DO
BEGIN
    -- Aggregate hourly metrics
    INSERT INTO agv_analytics_hourly
    SELECT 
        agv_id,
        DATE_FORMAT(ts, '%Y-%m-%d %H:00:00') AS hour_start,
        SUM(SQRT(
            POW(plant_x - LAG(plant_x) OVER (PARTITION BY agv_id ORDER BY ts), 2) +
            POW(plant_y - LAG(plant_y) OVER (PARTITION BY agv_id ORDER BY ts), 2)
        )) AS total_distance_m,
        AVG(speed_mps) AS avg_speed_mps,
        MAX(speed_mps) AS max_speed_mps,
        SUM(CASE WHEN speed_mps < 0.1 THEN 1 ELSE 0 END) / 3 AS idle_time_sec,
        SUM(CASE WHEN speed_mps >= 0.1 THEN 1 ELSE 0 END) / 3 AS moving_time_sec,
        0 AS charging_time_sec,
        0 AS task_count,
        0 AS zone_transitions,
        0 AS anomaly_count
    FROM agv_positions
    WHERE ts >= DATE_FORMAT(NOW() - INTERVAL 1 HOUR, '%Y-%m-%d %H:00:00')
    AND ts < DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00')
    GROUP BY agv_id, hour_start
    ON DUPLICATE KEY UPDATE
        total_distance_m = VALUES(total_distance_m),
        avg_speed_mps = VALUES(avg_speed_mps);
END;

CREATE EVENT IF NOT EXISTS daily_cleanup
ON SCHEDULE EVERY 1 DAY
STARTS '2025-01-01 02:00:00'
DO
  CALL ArchiveOldData(90);

CREATE EVENT IF NOT EXISTS zone_violation_check
ON SCHEDULE EVERY 1 MINUTE
DO
  CALL DetectZoneViolations(1);