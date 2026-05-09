"""Advanced federated queries: Neo4j UC JDBC aggregates + Delta sensor telemetry.

Combines live Neo4j graph aggregates (via remote_query) with Delta lakehouse
sensor readings for richer operational analytics. Uses the materialized Delta
tables from run_04 (neo4j_aircraft, neo4j_systems, neo4j_sensors, etc.) for
proper sensor→system→aircraft joins rather than REGEXP_EXTRACT approximations.

Prerequisites:
  - run_01_connect_test.py (UC JDBC connection + sensor_readings table)
  - run_04_materialized_tables.py (neo4j_aircraft, neo4j_systems, neo4j_sensors,
    neo4j_maintenance_events Delta tables)

Sections:
  1. Per-aircraft maintenance + sensor health
     Neo4j: maintenance counts by aircraft via remote_query
     Delta:  EGT and Vibration averages/max via sensor→system→aircraft join

Usage (via runner):
    uv run python -m cli upload run_06_advanced_federation.py
    uv run python -m cli submit run_06_advanced_federation.py
"""

import sys
import time

from data_utils import inject_params, get_config, ValidationResults


def main():
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    results = ValidationResults()
    conn = cfg["uc_connection_name"]
    fqn = cfg["fqn"]

    print("=" * 60)
    print("run_06_advanced_federation: Advanced Federated Queries")
    print("=" * 60)
    print(f"  Neo4j URI:     {cfg['neo4j_uri']}")
    print(f"  UC Connection: {conn}")
    print(f"  Schema:        {fqn}")
    print("")

    # ------------------------------------------------------------------
    # Section 1: Per-Aircraft Maintenance + Sensor Health
    #
    # Neo4j side: maintenance event counts (total and critical) per aircraft
    #             via two remote_query() GROUP BY calls.
    # Delta side: EGT and Vibration sensor averages and maximums per aircraft
    #             via proper sensor→system→aircraft join on materialized tables.
    # Result:     20-row fleet view with maintenance risk + sensor health side by side.
    # ------------------------------------------------------------------
    print("--- Section 1: Maintenance + Sensor Health per Aircraft ---")
    try:
        start = time.time()
        result = spark.sql(f"""
            WITH neo4j_maint AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT aircraftId, COUNT(*) AS total_events
                              FROM MaintenanceEvent
                              GROUP BY aircraftId')
            ),
            neo4j_critical AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT aircraftId, COUNT(*) AS critical
                              FROM MaintenanceEvent
                              WHERE severity = ''CRITICAL''
                              GROUP BY aircraftId')
            ),
            sensor_health AS (
                SELECT
                    sys.aircraftId,
                    ROUND(AVG(CASE WHEN sen.type = 'EGT'       THEN r.value END), 1) AS avg_egt,
                    ROUND(MAX(CASE WHEN sen.type = 'EGT'       THEN r.value END), 1) AS max_egt,
                    ROUND(AVG(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS avg_vibration,
                    ROUND(MAX(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS max_vibration
                FROM {fqn}.sensor_readings r
                JOIN {fqn}.neo4j_sensors sen ON r.sensorId = sen.sensorId
                JOIN {fqn}.neo4j_systems  sys ON sen.systemId = sys.systemId
                GROUP BY sys.aircraftId
            )
            SELECT
                a.tail_number,
                a.model,
                a.operator,
                COALESCE(m.total_events, 0)  AS maint_events,
                COALESCE(c.critical,    0)   AS critical,
                s.avg_egt       AS avg_egt_c,
                s.max_egt       AS max_egt_c,
                s.avg_vibration AS avg_vib_ips,
                s.max_vibration AS max_vib_ips
            FROM {fqn}.neo4j_aircraft a
            LEFT JOIN neo4j_maint    m ON a.aircraftId = m.aircraftId
            LEFT JOIN neo4j_critical c ON a.aircraftId = c.aircraftId
            LEFT JOIN sensor_health  s ON a.aircraftId = s.aircraftId
            ORDER BY maint_events DESC, critical DESC
        """)
        row_count = result.count()
        elapsed = (time.time() - start) * 1000
        result.show(10, truncate=False)
        results.record("Maintenance + sensor health per aircraft",
                       row_count == 20,
                       f"{row_count} aircraft, {elapsed:.0f}ms")
    except Exception as e:
        results.record("Maintenance + sensor health per aircraft", False, str(e)[:200])

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
