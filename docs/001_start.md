# Idea/plan

To investigate why merge-into with different partition+incremental strategies produce duplicates.

## Setup
* Use DuckDB as engine
* Setup DBT with duckdb - persistent please
* Create a seed datafile with the following properties:
    * tstamp_1 - "client-side"
    * derived_1 - client-side + a little jitter
    * collector - serverside - monotonic (although there can be duplicates)
    * load - load-side, monotonic but not unique
    * event_id - should be unique, but isn't
    * value - just to have something.
* The seed data should should have big enough range that we meaningfully can pretend to have multiple batches, and give reason to some overlap.
 
Build models as follows:
* seed (raw load of data)
* "staging" - takes data from seed based on load, partitions by either tstamp_1 or derived_1 (actually, let's try both and see if it makes a difference..?), de-dupes (using arbritrary) and writes
* downstream - takes from staging based on the tstamp_1 or derived_1, inserts based on event_id

* Command 1:
    dbt run --vars '{"start_date": "2025-10-01 17:00:00", "end_date": "2025-10-01 19:00:00"}'