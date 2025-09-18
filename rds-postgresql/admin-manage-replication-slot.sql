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

