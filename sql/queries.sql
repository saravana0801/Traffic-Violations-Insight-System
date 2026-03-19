-- ============================================================
-- queries.sql
-- Analytical SQL queries for Traffic Violations Insight System
-- Database: traffic_violations  |  Table: violations
-- ============================================================

USE traffic_violations;


-- ────────────────────────────────────────────────────────────
-- 1. SUMMARY KPIs
-- ────────────────────────────────────────────────────────────

-- Total violations
SELECT COUNT(*) AS total_violations
FROM violations;

-- Total accidents, fatalities, injuries
SELECT
    SUM(accident = 1)         AS total_accidents,
    SUM(fatal = 1)            AS total_fatalities,
    SUM(personal_injury = 1)  AS total_injuries,
    SUM(property_damage = 1)  AS total_property_damage,
    SUM(alcohol = 1)          AS alcohol_related,
    SUM(hazmat = 1)           AS hazmat_related
FROM violations;

-- Violation count by type
SELECT
    violation_type,
    COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM violations), 2) AS pct
FROM violations
GROUP BY violation_type
ORDER BY total DESC;


-- ────────────────────────────────────────────────────────────
-- 2. TOP LOCATIONS / AGENCIES
-- ────────────────────────────────────────────────────────────

-- Top 10 sub-agencies by violation count
SELECT
    sub_agency,
    COUNT(*) AS total_violations
FROM violations
GROUP BY sub_agency
ORDER BY total_violations DESC
LIMIT 10;

-- Top 15 road locations (intersections) with most incidents
SELECT
    location,
    COUNT(*) AS incident_count
FROM violations
WHERE location IS NOT NULL
GROUP BY location
ORDER BY incident_count DESC
LIMIT 15;

-- Top 10 locations with accidents
SELECT
    location,
    COUNT(*) AS accidents
FROM violations
WHERE accident = 1
GROUP BY location
ORDER BY accidents DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 3. TEMPORAL PATTERNS
-- ────────────────────────────────────────────────────────────

-- Violations by hour of day
SELECT
    stop_hour,
    COUNT(*) AS total
FROM violations
WHERE stop_hour IS NOT NULL
GROUP BY stop_hour
ORDER BY stop_hour;

-- Violations by day of week
SELECT
    day_of_week,
    COUNT(*) AS total
FROM violations
WHERE day_of_week IS NOT NULL
GROUP BY day_of_week
ORDER BY FIELD(day_of_week,
    'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');

-- Violations by month
SELECT
    month,
    COUNT(*) AS total
FROM violations
WHERE month IS NOT NULL
GROUP BY month
ORDER BY month;

-- Violations by time-of-day bucket
SELECT
    time_of_day,
    COUNT(*) AS total
FROM violations
WHERE time_of_day IS NOT NULL
GROUP BY time_of_day
ORDER BY FIELD(time_of_day, 'Morning','Afternoon','Evening','Night','Unknown');

-- Monthly trend by year
SELECT
    year_of_stop,
    month,
    COUNT(*) AS total
FROM violations
WHERE year_of_stop IS NOT NULL AND month IS NOT NULL
GROUP BY year_of_stop, month
ORDER BY year_of_stop, month;


-- ────────────────────────────────────────────────────────────
-- 4. DEMOGRAPHICS
-- ────────────────────────────────────────────────────────────

-- Violations by race
SELECT
    race,
    COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM violations), 2) AS pct
FROM violations
WHERE race IS NOT NULL
GROUP BY race
ORDER BY total DESC;

-- Violations by gender
SELECT
    gender,
    COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM violations), 2) AS pct
FROM violations
WHERE gender IS NOT NULL
GROUP BY gender
ORDER BY total DESC;

-- Race × Violation Type cross-tab
SELECT
    race,
    violation_type,
    COUNT(*) AS total
FROM violations
WHERE race IS NOT NULL AND violation_type IS NOT NULL
GROUP BY race, violation_type
ORDER BY race, total DESC;

-- Gender × Accident breakdown
SELECT
    gender,
    SUM(accident = 1) AS accidents,
    COUNT(*)          AS total_stops,
    ROUND(SUM(accident = 1) * 100.0 / COUNT(*), 2) AS accident_rate_pct
FROM violations
WHERE gender IN ('M','F')
GROUP BY gender;


-- ────────────────────────────────────────────────────────────
-- 5. VEHICLE ANALYSIS
-- ────────────────────────────────────────────────────────────

-- Top 15 vehicle makes
SELECT
    make,
    COUNT(*) AS total
FROM violations
WHERE make IS NOT NULL
GROUP BY make
ORDER BY total DESC
LIMIT 15;

-- Vehicle type distribution
SELECT
    vehicle_type,
    COUNT(*) AS total
FROM violations
WHERE vehicle_type IS NOT NULL
GROUP BY vehicle_type
ORDER BY total DESC;

-- Most cited vehicle make in accidents
SELECT
    make,
    COUNT(*) AS accident_count
FROM violations
WHERE accident = 1 AND make IS NOT NULL
GROUP BY make
ORDER BY accident_count DESC
LIMIT 10;

-- Vehicle manufacture year distribution
SELECT
    vehicle_year,
    COUNT(*) AS total
FROM violations
WHERE vehicle_year IS NOT NULL
GROUP BY vehicle_year
ORDER BY vehicle_year;


-- ────────────────────────────────────────────────────────────
-- 6. SEVERITY & SAFETY
-- ────────────────────────────────────────────────────────────

-- Severity breakdown per sub-agency
SELECT
    sub_agency,
    COUNT(*)                    AS total_stops,
    SUM(accident = 1)           AS accidents,
    SUM(fatal = 1)              AS fatalities,
    SUM(personal_injury = 1)    AS injuries,
    SUM(alcohol = 1)            AS alcohol_cases
FROM violations
WHERE sub_agency IS NOT NULL
GROUP BY sub_agency
ORDER BY accidents DESC
LIMIT 15;

-- Fatal accidents with contributing details
SELECT
    date_of_stop,
    location,
    sub_agency,
    race,
    gender,
    make,
    model,
    vehicle_year,
    charge
FROM violations
WHERE fatal = 1
ORDER BY date_of_stop DESC
LIMIT 50;

-- Alcohol-related violations by hour
SELECT
    stop_hour,
    COUNT(*) AS alcohol_violations
FROM violations
WHERE alcohol = 1 AND stop_hour IS NOT NULL
GROUP BY stop_hour
ORDER BY stop_hour;


-- ────────────────────────────────────────────────────────────
-- 7. SEARCH & ENFORCEMENT
-- ────────────────────────────────────────────────────────────

-- Search conducted breakdown
SELECT
    search_conducted,
    COUNT(*) AS total
FROM violations
GROUP BY search_conducted;

-- Search outcomes
SELECT
    search_outcome,
    COUNT(*) AS total
FROM violations
WHERE search_outcome IS NOT NULL AND search_outcome != 'Not Applicable'
GROUP BY search_outcome
ORDER BY total DESC;

-- Arrest type distribution
SELECT
    arrest_type,
    COUNT(*) AS total
FROM violations
WHERE arrest_type IS NOT NULL
GROUP BY arrest_type
ORDER BY total DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 8. HIGH-RISK ZONES (coordinates available)
-- ────────────────────────────────────────────────────────────

-- Locations with valid GPS that have accidents
SELECT
    location,
    ROUND(AVG(latitude),  6) AS avg_lat,
    ROUND(AVG(longitude), 6) AS avg_lon,
    COUNT(*)                 AS total_incidents,
    SUM(accident = 1)        AS accidents
FROM violations
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND accident = 1
GROUP BY location
ORDER BY accidents DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 9. DAILY SUMMARY VIEW (uses pre-created view)
-- ────────────────────────────────────────────────────────────

SELECT *
FROM vw_daily_summary
ORDER BY date_of_stop DESC
LIMIT 30;
