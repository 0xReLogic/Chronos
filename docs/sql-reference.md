# ChronosDB SQL Reference

## Document Purpose

Complete SQL syntax reference for ChronosDB, covering supported statements, data types, operators, functions, and limitations.

---

## System Overview

ChronosDB implements a subset of SQL optimized for edge/IoT workloads. The focus is on simple CRUD operations, time-series aggregations, and data retention rather than complex analytical queries.

**Design Philosophy:**
- Small parser footprint (Pest grammar)
- Fast execution for sensor data patterns
- Offline-first compatibility
- Resource-constrained device support

---

## Data Types

### Supported Types

| Type | Description | Storage | Example |
|------|-------------|---------|---------|
| `INT` | 64-bit signed integer | 8 bytes | `42`, `-100` |
| `FLOAT` | 64-bit floating point | 8 bytes | `25.5`, `-3.14` |
| `STRING` | UTF-8 text | Variable | `'sensor-01'` |
| `BOOLEAN` | True/false | 1 byte | `TRUE`, `FALSE` |

### Type Aliases

- `TEXT` → `STRING`

### NULL Values

All columns support NULL by default:

```sql
INSERT INTO sensors (id, temp) VALUES (1, NULL);
SELECT * FROM sensors WHERE temp IS NULL;
```

---

## DDL (Data Definition Language)

### CREATE TABLE

**Syntax:**
```sql
CREATE TABLE table_name (
    column_name data_type,
    column_name data_type,
    ...
) [WITH TTL=duration];
```

**Examples:**

```sql
-- Basic table
CREATE TABLE sensors (
    id INT,
    temperature FLOAT,
    device STRING
);

-- Table with TTL (7 days)
CREATE TABLE events (
    timestamp INT,
    event_type STRING,
    value FLOAT
) WITH TTL=7d;

-- Supported TTL units: s (seconds), m (minutes), h (hours), d (days)
CREATE TABLE logs (message STRING) WITH TTL=24h;
CREATE TABLE metrics (value FLOAT) WITH TTL=3600s;
```

**TTL Behavior:**
- Rows expire based on insert time
- Background cleanup runs every `ttl_cleanup_interval_secs` (default: 3600s)
- Expired rows deleted in batches (default: 1000 per pass)
- TTL metadata stored in `__ttl_index__` tree

**Limitations:**
- No PRIMARY KEY constraint
- No FOREIGN KEY constraint
- No UNIQUE constraint
- No DEFAULT values
- No AUTO_INCREMENT

---

### CREATE INDEX

**Syntax:**
```sql
CREATE INDEX index_name ON table_name(column_name);
```

**Examples:**

```sql
CREATE INDEX idx_device ON sensors(device);
CREATE INDEX idx_timestamp ON events(timestamp);
```

**Index Behavior:**
- Secondary B-tree index stored in separate Sled tree
- Automatically maintained on INSERT/DELETE
- Used for equality filters: `WHERE column = value`
- No range scan optimization yet

**Limitations:**
- Single-column indexes only
- No composite indexes
- No covering indexes
- No index hints

---

## DML (Data Manipulation Language)

### INSERT

**Syntax:**
```sql
INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...);
INSERT INTO table_name VALUES (value1, value2, ...);  -- All columns
```

**Examples:**

```sql
-- Explicit columns
INSERT INTO sensors (id, temperature, device) VALUES (1, 25.5, 'sensor-01');

-- All columns (order matches CREATE TABLE)
INSERT INTO sensors VALUES (2, 30.0, 'sensor-02');

-- NULL values
INSERT INTO sensors (id, device) VALUES (3, 'sensor-03');  -- temperature = NULL
```

**Behavior:**
- Single-row insert only (no batch INSERT)
- Distributed mode: routed through Raft consensus
- Triggers aggregation bucket updates (for FLOAT columns)
- Triggers offline queue append (if sync enabled)

**Limitations:**
- No multi-row INSERT: `VALUES (...), (...)`
- No INSERT ... SELECT
- No INSERT ... ON CONFLICT

---

### SELECT

**Syntax:**
```sql
SELECT column1, column2, ... FROM table_name [WHERE condition];
SELECT * FROM table_name [WHERE condition];
```

**Examples:**

```sql
-- All columns
SELECT * FROM sensors;

-- Specific columns
SELECT id, temperature FROM sensors;

-- With filter
SELECT * FROM sensors WHERE device = 'sensor-01';
SELECT * FROM sensors WHERE temperature > 25.0;
SELECT * FROM sensors WHERE id >= 10 AND id <= 20;
```

**Supported WHERE Operators:**
- `=` (equals)
- `!=`, `<>` (not equals)
- `<` (less than)
- `<=` (less than or equal)
- `>` (greater than)
- `>=` (greater than or equal)

**Limitations:**
- No JOINs
- No subqueries
- No GROUP BY / HAVING
- No ORDER BY
- No LIMIT / OFFSET
- No DISTINCT
- No aggregate functions (SUM, COUNT, AVG) except time-window variants

---

### UPDATE

**Syntax:**
```sql
UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition;
```

**Examples:**

```sql
UPDATE sensors SET temperature = 26.0 WHERE id = 1;
UPDATE sensors SET device = 'sensor-updated' WHERE device = 'sensor-01';
```

**Behavior:**
- Fetch matching rows
- Apply assignments
- Delete old rows
- Insert updated rows

**Limitations:**
- WHERE clause required (no UPDATE without filter)
- No UPDATE ... FROM
- No computed expressions in SET clause

---

### DELETE

**Syntax:**
```sql
DELETE FROM table_name WHERE condition;
```

**Examples:**

```sql
DELETE FROM sensors WHERE id = 1;
DELETE FROM sensors WHERE temperature < 0;
```

**Behavior:**
- Removes rows matching WHERE condition
- Updates secondary indexes
- Removes from TTL index

**Limitations:**
- WHERE clause required (no DELETE without filter)
- No DELETE ... USING
- No TRUNCATE TABLE

---

## Time-Window Aggregations

ChronosDB provides specialized aggregate functions for time-series data:

### AVG_1H

**Syntax:**
```sql
SELECT AVG_1H(column_name) FROM table_name;
```

**Description:** Average of FLOAT column over last 1 hour

**Example:**
```sql
SELECT AVG_1H(temperature) FROM sensors;
```

**Output:**
```
+---------------------+
| avg_1h(temperature) |
+---------------------+
| 25.75               |
+---------------------+
```

---

### AVG_24H

**Syntax:**
```sql
SELECT AVG_24H(column_name) FROM table_name;
```

**Description:** Average of FLOAT column over last 24 hours

**Example:**
```sql
SELECT AVG_24H(temperature) FROM sensors;
```

---

### AVG_7D

**Syntax:**
```sql
SELECT AVG_7D(column_name) FROM table_name;
```

**Description:** Average of FLOAT column over last 7 days

**Example:**
```sql
SELECT AVG_7D(temperature) FROM sensors;
```

---

### Aggregation Internals

**Bucket Structure:**
- 1-hour window: 60 one-minute buckets
- 24-hour window: 24 one-hour buckets
- 7-day window: 7 one-day buckets

**Update Mechanism:**
- On INSERT, FLOAT columns update corresponding buckets
- Buckets stored in-memory and persisted to Sled tree `__agg_state__`

**Limitations:**
- Only FLOAT columns supported
- No WHERE clause filtering
- No GROUP BY
- No multi-column aggregations
- Returns single row with single column

---

## Transactions

**Syntax:**
```sql
BEGIN;
-- SQL statements
COMMIT;

-- Or rollback
BEGIN;
-- SQL statements
ROLLBACK;
```

**Examples:**

```sql
BEGIN;
INSERT INTO sensors VALUES (10, 25.0, 'sensor-10');
INSERT INTO sensors VALUES (11, 26.0, 'sensor-11');
COMMIT;
```

**Behavior:**
- Single-node mode: Statements buffered until COMMIT
- Distributed mode: Transactions not supported (each statement is atomic)

**Limitations:**
- No distributed transactions
- No isolation levels
- No savepoints
- Single-row ACID only in distributed mode

---

## SQL Examples

### IoT Sensor Data

```sql
-- Create table with 7-day retention
CREATE TABLE sensor_readings (
    sensor_id INT,
    timestamp INT,
    temperature FLOAT,
    humidity FLOAT,
    location STRING
) WITH TTL=7d;

-- Create index for fast sensor lookup
CREATE INDEX idx_sensor ON sensor_readings(sensor_id);

-- Insert readings
INSERT INTO sensor_readings VALUES (1, 1704067200, 25.5, 60.0, 'warehouse-A');
INSERT INTO sensor_readings VALUES (2, 1704067260, 30.0, 55.0, 'warehouse-B');

-- Query specific sensor
SELECT * FROM sensor_readings WHERE sensor_id = 1;

-- Get 1-hour average temperature
SELECT AVG_1H(temperature) FROM sensor_readings;
```

---

### Device Inventory

```sql
-- Create device table
CREATE TABLE devices (
    device_id INT,
    device_name STRING,
    status STRING,
    last_seen INT
);

-- Insert devices
INSERT INTO devices VALUES (1, 'gateway-01', 'online', 1704067200);
INSERT INTO devices VALUES (2, 'gateway-02', 'offline', 1704063600);

-- Update device status
UPDATE devices SET status = 'online', last_seen = 1704070800 WHERE device_id = 2;

-- Remove old devices
DELETE FROM devices WHERE last_seen < 1704000000;
```

---

### Event Logging

```sql
-- Create event log with 24-hour retention
CREATE TABLE events (
    event_id INT,
    event_type STRING,
    severity STRING,
    message STRING,
    timestamp INT
) WITH TTL=24h;

-- Log events
INSERT INTO events VALUES (1, 'error', 'high', 'Sensor disconnected', 1704067200);
INSERT INTO events VALUES (2, 'warning', 'medium', 'High temperature', 1704067260);

-- Query errors
SELECT * FROM events WHERE severity = 'high';
```

---

## Query Optimization

### Use Indexes

**Slow (full table scan):**
```sql
SELECT * FROM sensors WHERE device = 'sensor-01';
```

**Fast (index scan):**
```sql
CREATE INDEX idx_device ON sensors(device);
SELECT * FROM sensors WHERE device = 'sensor-01';
```

**Performance:**
- Full scan: ~9ms for 1000 rows
- Index scan: <5ms for 1000 rows

---

### Limit Result Sets

ChronosDB does not support LIMIT, so filter aggressively:

**Inefficient:**
```sql
SELECT * FROM sensors;  -- Returns all rows
```

**Efficient:**
```sql
SELECT * FROM sensors WHERE timestamp > 1704063600;  -- Last hour only
```

---

### Use Time-Window Aggregations

**Slow (full scan + client-side aggregation):**
```sql
-- Not supported in ChronosDB
SELECT AVG(temperature) FROM sensors WHERE timestamp > NOW() - 3600;
```

**Fast (pre-computed buckets):**
```sql
SELECT AVG_1H(temperature) FROM sensors;
```

**Performance:**
- Full scan + aggregation: ~36ms for 1000 rows
- Time-window aggregation: <10ms

---

## Limitations

### Not Supported

**Advanced SQL:**
- JOINs (INNER, LEFT, RIGHT, FULL OUTER)
- Subqueries
- CTEs (WITH clause)
- Window functions (ROW_NUMBER, RANK, etc.)
- UNION / INTERSECT / EXCEPT

**Aggregations:**
- GROUP BY / HAVING
- General aggregate functions (SUM, COUNT, MIN, MAX, AVG)
- DISTINCT

**Query Modifiers:**
- ORDER BY
- LIMIT / OFFSET
- FOR UPDATE

**DDL:**
- ALTER TABLE
- DROP TABLE
- DROP INDEX
- TRUNCATE TABLE

**Constraints:**
- PRIMARY KEY
- FOREIGN KEY
- UNIQUE
- CHECK
- NOT NULL

**Data Types:**
- BLOB / BINARY
- DATE / TIME / DATETIME (use INT for Unix timestamps)
- JSON
- ARRAY
- ENUM

**Functions:**
- String functions (CONCAT, SUBSTRING, etc.)
- Math functions (ROUND, CEIL, FLOOR, etc.)
- Date functions (NOW, DATE_ADD, etc.)
- User-defined functions

---

## SQL Grammar

ChronosDB uses Pest parser with the following grammar (simplified):

```pest
statement = { create_table | insert | select | update | delete | create_index }

create_table = { "CREATE" ~ "TABLE" ~ identifier ~ "(" ~ column_list ~ ")" ~ ttl_clause? }
column_list = { column_def ~ ("," ~ column_def)* }
column_def = { identifier ~ data_type }
data_type = { "INT" | "FLOAT" | "STRING" | "TEXT" | "BOOLEAN" }
ttl_clause = { "WITH" ~ "TTL" ~ "=" ~ duration }

insert = { "INSERT" ~ "INTO" ~ identifier ~ column_list? ~ "VALUES" ~ value_list }
select = { "SELECT" ~ column_list ~ "FROM" ~ identifier ~ where_clause? }
update = { "UPDATE" ~ identifier ~ "SET" ~ assignment_list ~ where_clause }
delete = { "DELETE" ~ "FROM" ~ identifier ~ where_clause }

where_clause = { "WHERE" ~ condition }
condition = { identifier ~ operator ~ value }
operator = { "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" }
```

Full grammar: `src/parser/chronos.pest`

---

## Best Practices

### Schema Design

1. **Use appropriate data types:**
   - INT for IDs and timestamps
   - FLOAT for sensor readings
   - STRING for device names and metadata

2. **Set TTL for time-series data:**
   ```sql
   CREATE TABLE metrics (...) WITH TTL=7d;
   ```

3. **Create indexes on filter columns:**
   ```sql
   CREATE INDEX idx_device ON sensors(device);
   ```

---

### Query Patterns

1. **Always use WHERE clause for DELETE/UPDATE:**
   ```sql
   DELETE FROM sensors WHERE id = 1;  -- Good
   DELETE FROM sensors;                -- Not supported
   ```

2. **Prefer time-window aggregations:**
   ```sql
   SELECT AVG_1H(temp) FROM sensors;  -- Fast
   ```

3. **Filter early:**
   ```sql
   SELECT * FROM sensors WHERE device = 'sensor-01';  -- Good
   SELECT * FROM sensors;  -- Avoid if table is large
   ```

---

### Data Retention

1. **Use TTL for automatic cleanup:**
   ```sql
   CREATE TABLE logs (...) WITH TTL=24h;
   ```

2. **Manual cleanup for non-TTL tables:**
   ```sql
   DELETE FROM events WHERE timestamp < 1704000000;
   ```

3. **Monitor disk usage:**
   ```bash
   du -sh /var/lib/chronos/*
   ```

---

This SQL reference covers all supported features in ChronosDB v1.0.0.
