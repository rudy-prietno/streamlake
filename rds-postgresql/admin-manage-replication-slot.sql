-- ##################################

-- max_replication_slots
-- What it means: Maximum number of replication slots available.

-- wal_level
-- What it means: Defines how much detail PostgreSQL writes to the transaction log (WAL)

-- max_wal_senders
-- What it means: Maximum number of WAL sender processes that stream data to replicas or subscribers.

-- hot_standby
-- What it means: Whether read queries are allowed on replica servers.

-- ##################################
select
    name,
    setting
from pg_settings
where name in (
            'max_replication_slots',
            'wal_level',
            'max_wal_senders',
            'hot_standby'
            );


-- ##################################

-- What it does: Creates a logical replication slot named cdc_databasename_slot using the pgoutput plugin.
-- Replication slots guarantee reliability in streaming changes to external systems.

-- ##################################
SELECT * 
FROM pg_create_logical_replication_slot('cdc_databasename_slot', 'pgoutput');


-- ##################################

-- What it does: Deletes the replication slot.
-- Important: Always drop unused slots; otherwise, old WAL logs may pile up, leading to high storage costs.

-- ##################################
SELECT pg_drop_replication_slot('cdc_databasename_slot');


-- ##################################

-- The unique name of the replication slot.
-- The output plugin used for logical replication.
    -- Common values:
    -- pgoutput → built-in logical replication (standard).

-- The database where the slot was created.
-- true if a client (replica or CDC tool) is currently connected and consuming the slot.
-- Process ID of the active connection using this slot.
-- The WAL (log sequence number) position that must be kept to support this slot.
-- The WAL position confirmed as processed by the client.

-- ##################################
SELECT
  slot_name,
  plugin,
  database,
  active,
  active_pid,
  restart_lsn,
  confirmed_flush_lsn
FROM pg_replication_slots


-- ##################################

-- A publication defines what data changes leave the database.
-- This setup is required for CDC pipelines to capture inserts, updates, and deletes.
    -- Fine-grained control means:
        -- Only business-critical tables are replicated.
        -- We can avoid unnecessary storage and cost by excluding irrelevant tables.

-- cdc_pub_database_prod → the identifier for this publication.
-- public.table_name (listed multiple times here, but typically you would include one or more distinct tables).
-- (publish = 'insert, update, delete') → the operations that will be replicated.
    -- Options include:
        -- insert → new rows.
        -- update → modified rows.
        -- delete → removed rows.


-- ##################################
CREATE PUBLICATION cdc_pub_database_prod
FOR TABLE
    public.table_name,
    public.table_name
WITH (publish = 'insert, update, delete');


-- ##################################



-- ##################################
