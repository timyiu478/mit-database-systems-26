# Problem Set 1

## Setting up the starter code

See `pset1.pdf` for more descriptions of the data and the questions.

Run `make starter` in this directory. The starter code zip contains ten `.sql`
files, `q1.sql` to `q10.sql`. Each file has a comment at the top that indicates
the question number (e.g., `-- Q1`).

## Generating the data

### 1. Download the base data from the MBTA.

Download the database (`mbta.sqlite`) from https://www.dropbox.com/scl/fi/mvp5zz5257j1zxt27embn/mbta.sqlite?rlkey=mnef0mpryahw8vfz11s8rmyyt&st=oa48q89f&dl=0

### 2. Resulting dataset

#### Schema
```sql
sqlite> .tables
gated_station_entries  routes                 time_periods
lines                  station_orders
rail_ridership         stations

sqlite> .schema gated_station_entries
CREATE TABLE gated_station_entries (
  service_date TEXT,
  time TEXT,
  station_id TEXT,
  line_id TEXT,
  gated_entries REAL,
  PRIMARY KEY (service_date, time, station_id, line_id)
);

sqlite> .schema lines
CREATE TABLE lines (
  line_id TEXT,
  line_name TEXT,
  PRIMARY KEY (line_id)
);

sqlite> .schema rail_ridership
CREATE TABLE rail_ridership (
  season TEXT,
  line_id TEXT,
  direction INTEGER,
  time_period_id TEXT,
  station_id TEXT,
  total_ons INTEGER,
  total_offs INTEGER,
  number_service_days INTEGER,
  average_ons INTEGER,
  average_offs INTEGER,
  average_flow INTEGER,
  PRIMARY KEY (season, line_id, direction, time_period_id, station_id)
);

sqlite> .schema routes
CREATE TABLE routes (
  route_id INTEGER,
  line_id TEXT,
  first_station_id TEXT,
  last_station_id TEXT,
  direction INTEGER,
  direction_desc TEXT,
  route_name TEXT,
  PRIMARY KEY (route_id)
);

sqlite> .schema station_orders
CREATE TABLE station_orders (
  route_id INTEGER,
  station_id TEXT,
  stop_order INTEGER,
  distance_from_last_station_miles REAL,
  PRIMARY KEY (route_id, station_id)
);

sqlite> .schema stations
CREATE TABLE stations (
  station_id TEXT,
  station_name TEXT,
  PRIMARY KEY (station_id)
);

sqlite> .schema time_periods
CREATE TABLE time_periods (
  time_period_id TEXT,
  day_type TEXT,
  time_period TEXT,
  period_start_time TEXT,
  period_end_time TEXT,
  PRIMARY KEY (time_period_id)
);
```

#### Cardinalities

```sql
sqlite> SELECT COUNT(*) FROM gated_station_entries;
8483703

sqlite> SELECT COUNT(*) FROM lines;
5

sqlite> SELECT COUNT(*) FROM rail_ridership;
7854

sqlite> SELECT COUNT(*) FROM routes;
18

sqlite> SELECT COUNT(*) FROM station_orders;
326

sqlite> SELECT COUNT(*) FROM stations;
120

sqlite> SELECT COUNT(*) FROM time_periods;
11
```
