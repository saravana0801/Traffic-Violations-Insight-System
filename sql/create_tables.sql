-- ============================================================
-- create_tables.sql
-- MySQL schema for Traffic Violations Insight System
-- Run this manually OR it is executed automatically by db_loader.py
-- ============================================================

CREATE DATABASE IF NOT EXISTS traffic_violations
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE traffic_violations;

-- ── Main violations table ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS violations (
    id                      BIGINT          UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    seq_id                  VARCHAR(50)     NULL,
    date_of_stop            DATE            NULL,
    time_of_stop            TIME            NULL,
    agency                  VARCHAR(50)     NULL,
    sub_agency              VARCHAR(120)    NULL,
    description             TEXT            NULL,
    location                VARCHAR(255)    NULL,
    latitude                DECIMAL(10, 7)  NULL,
    longitude               DECIMAL(10, 7)  NULL,

    -- Boolean / incident flags
    accident                TINYINT(1)      NULL COMMENT '1=True 0=False',
    belts                   TINYINT(1)      NULL,
    personal_injury         TINYINT(1)      NULL,
    property_damage         TINYINT(1)      NULL,
    fatal                   TINYINT(1)      NULL,
    commercial_license      TINYINT(1)      NULL,
    hazmat                  TINYINT(1)      NULL,
    commercial_vehicle      TINYINT(1)      NULL,
    alcohol                 TINYINT(1)      NULL,
    work_zone               TINYINT(1)      NULL,
    search_conducted        TINYINT(1)      NULL,
    contributed_to_accident TINYINT(1)      NULL,

    -- Search fields
    search_disposition      VARCHAR(100)    NULL,
    search_outcome          VARCHAR(100)    NULL,
    search_reason           VARCHAR(150)    NULL,
    search_reason_for_stop  VARCHAR(150)    NULL,
    search_type             VARCHAR(100)    NULL,
    search_arrest_reason    VARCHAR(150)    NULL,

    -- Geographic / driver info
    state                   CHAR(2)         NULL,
    driver_city             VARCHAR(100)    NULL,
    driver_state            CHAR(2)         NULL,
    dl_state                CHAR(2)         NULL,

    -- Vehicle info
    vehicle_type            VARCHAR(100)    NULL,
    vehicle_year            SMALLINT        NULL,
    make                    VARCHAR(60)     NULL,
    model                   VARCHAR(60)     NULL,
    color                   VARCHAR(40)     NULL,

    -- Violation / charge info
    violation_type          VARCHAR(60)     NULL,
    charge                  VARCHAR(120)    NULL,
    article                 VARCHAR(120)    NULL,
    arrest_type             VARCHAR(120)    NULL,

    -- Driver demographics
    race                    VARCHAR(50)     NULL,
    gender                  CHAR(10)        NULL,

    -- Engineered features
    stop_hour               TINYINT         NULL COMMENT '0-23',
    time_of_day             VARCHAR(20)     NULL COMMENT 'Morning/Afternoon/Evening/Night',
    day_of_week             VARCHAR(15)     NULL,
    month                   TINYINT         NULL COMMENT '1-12',
    year_of_stop            SMALLINT        NULL,

    -- Indexes for common filter columns
    INDEX idx_date          (date_of_stop),
    INDEX idx_violation     (violation_type),
    INDEX idx_race          (race),
    INDEX idx_gender        (gender),
    INDEX idx_make          (make),
    INDEX idx_sub_agency    (sub_agency),
    INDEX idx_accident      (accident),
    INDEX idx_hour          (stop_hour),
    INDEX idx_month         (month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ── Summary / aggregation view ────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_daily_summary AS
SELECT
    date_of_stop,
    COUNT(*)                                        AS total_violations,
    SUM(accident = 1)                               AS total_accidents,
    SUM(fatal = 1)                                  AS total_fatalities,
    SUM(personal_injury = 1)                        AS total_injuries,
    SUM(alcohol = 1)                                AS alcohol_related,
    COUNT(DISTINCT sub_agency)                      AS agencies_active
FROM violations
GROUP BY date_of_stop
ORDER BY date_of_stop;
