OnlineSchemaChange.php and osc_wrapper.php

Overview and limitations

OnlineSchemaChange (OSC) was built by Facebook Database Engineering to address the need to be able to alter MySQL table schema with minimal impact in a highly available sharded environment. To reduce the barriers to entry of using OSC, Facebook Database Operations created a command line script called osc_wrapper.

A simplification of the process is:

1. A series of triggers are created that record changes to the table to be altered.

2. The data of the original table is copied into a new table with the desired schema.

3. The changes recorded by the triggers are applied.

4. The new table is swapped into place.

OSC has a number of limitations, a subset of which are:

The table to be altered may not already have triggers or foreign keys. Foreign keys may be supported at some point in the future (patches welcomed).

The script allows for a mostly (but not fully) online schema change. The application should be able to accept brief locking with a relatively small number of queries that will fail.

The alteration will create additional load on the server. It is often a good idea to run OSC off hours, if possible.

In general, the table to be altered should have a Primary Key.

OSC changes are not replicated from master to replica. Also, the script should not be run on a master and replica at the same time, as it will cause ephemeral replication errors.

OSC is completely untested with row based replication and will probably not work.

On a server with a large number of tables the foreign key checks can be extremely expensive. These checks take place in the osc_wrapper and may be skipped.

OSC and xtrabackup/InnoDB Hot Backup should not be run at the same time.

OSC does not support renaming columns.

OSC only works on MySQL 5.0 and 5.1 and it has been a while since we ran it on 5.0. You should run the unit tests. Example passing unit test output is attached.

All development and testing has occurred on Linux. Running on non-Linux systems will probably be a challenge.

Getting started

The osc_wrapper accepts either CREATE TABLE or ALTER TABLE statements. ALTER TABLE statements are converted to CREATE TABLE statements using a scratch schema (`test` by default). Alternatively, a MySQL host can be supplied that will act as an external reference for what the desired schema is.

By default the osc_wrapper acts on schemas not like test, mysql, localinfo, snapshot%, %_restored. For each schema, the current schema is examined. If the desired and existing schema appear to be the same, then the alterations will be skipped.

A few examples:

#php osc_wrapper.php --mode=seed --create_missing_table --seed_host=dev123.server.com --seed_db=staging --seed_tables=groups,transactions

In the above example osc_wrapper will pull in the table definitions of the tables named groups and transactions from the staging schema on dev123.server.com . As a schema to alter is not specified, all schemas that do not match the patterns noted above will be altered. If the tables do not exist, they will be created.

#php osc_wrapper.php --mode=statement --ddl_file=create.sql --dbname=production

In this case, osc_wrapper will read in ALTER or CREATE statements from a file create.sql and then attempt to alter the schema of tables in production schema.

# php osc_wrapper.php --mode=clean

Something very bad has happened and you want to kill the current OSC operation and clean up orphaned triggers, tables,etc.