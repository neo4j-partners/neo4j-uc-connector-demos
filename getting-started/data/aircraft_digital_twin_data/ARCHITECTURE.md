# Aircraft Digital Twin Data Architecture

This directory contains the synthetic aircraft digital twin dataset used by the
Neo4j + Databricks Unity Catalog federation examples.

## Dataset Summary

- 20 aircraft across four operators
- 80 aircraft systems
- 320 components
- 160 sensors
- 172,800 sensor readings
- 800 flights
- 300 maintenance events
- 514 delay records
- 12 airports

Sensor readings cover 45 days of hourly telemetry from July 1, 2024 through
August 14, 2024. The CSV files are the import format; the notebooks and
validation scripts normalize identifier columns to the repo-wide camelCase
convention when creating graph properties and Delta tables.

## Storage Model

Neo4j stores graph-native topology and operational entities:

- `Aircraft`
- `Airport`
- `System`
- `Component`
- `Sensor`
- `Flight`
- `MaintenanceEvent`
- `Delay`

Databricks Delta stores the lakehouse tables used for analytics and joins:

- `aircraft`
- `systems`
- `sensors`
- `sensor_readings`

The `sensor_readings` table is not loaded into Neo4j. It is created directly in
Delta from `nodes_readings.csv` by `getting-started/01-simple-connect-test.ipynb`.

## Naming Conventions

CSV import headers use Neo4j import-style names and snake_case source names,
for example `:ID(Aircraft)`, `aircraft_id`, and `sensor_id`.

Runtime graph properties and Delta table columns use camelCase identifiers:

| Concept | CSV Source | Runtime Column or Property |
|---------|------------|----------------------------|
| Aircraft ID | `:ID(Aircraft)` / `aircraft_id` | `aircraftId` |
| System ID | `:ID(System)` / `system_id` | `systemId` |
| Sensor ID | `:ID(Sensor)` / `sensor_id` | `sensorId` |
| Reading ID | `reading_id` | `readingId` |
| Flight ID | `:ID(Flight)` / `flight_id` | `flightId` |
| Maintenance event ID | `:ID(MaintenanceEvent)` | `eventId` |

Stable descriptive fields that are already depended on by examples remain
snake_case, such as `tail_number`, `flight_number`, `reported_at`, and
`corrective_action`.

## CSV Files

Node files:

| File | Runtime Entity | Count |
|------|----------------|-------|
| `nodes_aircraft.csv` | `Aircraft` / `aircraft` | 20 |
| `nodes_airports.csv` | `Airport` | 12 |
| `nodes_systems.csv` | `System` / `systems` | 80 |
| `nodes_components.csv` | `Component` | 320 |
| `nodes_sensors.csv` | `Sensor` / `sensors` | 160 |
| `nodes_flights.csv` | `Flight` | 800 |
| `nodes_maintenance.csv` | `MaintenanceEvent` | 300 |
| `nodes_delays.csv` | `Delay` | 514 |
| `nodes_readings.csv` | `sensor_readings` Delta table | 172,800 |

Relationship files:

| File | Relationship Type | Count |
|------|-------------------|-------|
| `rels_aircraft_system.csv` | `HAS_SYSTEM` | 80 |
| `rels_system_component.csv` | `HAS_COMPONENT` | 320 |
| `rels_system_sensor.csv` | `HAS_SENSOR` | 160 |
| `rels_aircraft_flight.csv` | `OPERATES_FLIGHT` | 800 |
| `rels_flight_departure.csv` | `DEPARTS_FROM` | 800 |
| `rels_flight_arrival.csv` | `ARRIVES_AT` | 800 |
| `rels_flight_delay.csv` | `HAS_DELAY` | 514 |
| `rels_component_event.csv` | `HAS_EVENT` | 300 |

## Query Pattern

The main federation pattern traverses aircraft topology in Neo4j, then joins the
result to Delta sensor telemetry:

```sql
SELECT
  sys.aircraftId,
  sen.type,
  AVG(r.value) AS avg_value
FROM sensor_readings r
JOIN sensors sen ON r.sensorId = sen.sensorId
JOIN systems sys ON sen.systemId = sys.systemId
GROUP BY sys.aircraftId, sen.type
```

For setup and loading instructions, see
[`getting-started/README.md`](../../README.md) and
[`validation/README.md`](../../../validation/README.md).
