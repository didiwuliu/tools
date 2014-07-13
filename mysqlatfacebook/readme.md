Online Schema Change for MySQL
By Mark Callaghan on Wednesday, September 15, 2010 at 11:49am
It is great to be able to build small utilities on top of an excellent RDBMS. Thank you MySQL. 
 
This is a small but complex utility to perform online schema change for MySQL. We call it OSC and the source is here.
 
Some ALTER TABLE statements take too long form the perspective of some MySQL users. The fast index create feature for the InnoDB plugin in MySQL 5.1 makes this less of an issue but this can still take minutes to hours for a large table and for some MySQL deployments that is too long.
 
A workaround is to perform the change on a slave first and then promote the slave to be the new master. But this requires a slave located near the master. MySQL 5.0 added support for triggers and some replication systems have been built using triggers to capture row changes. Why not use triggers for this? The openarkkit toolkit did just that with oak-online-alter-table. We have published our version of an online schema change utility (OnlineSchemaChange.php aka OSC).
 
The remainder of this document is copied from the internal documents that were written for this project. Note that this project was done by Vamsi and he did an amazing job with it. In addition to writing the tool, writing the docs and doing a lot of testing he also found and fixed or avoided a few bugs in MySQL to make sure OSC would be reliable.
 
Overview
If the row format of database allows addition of a new column (possibly positioned at the end of existing columns with some default value) without modifying every row of the table, addition of a column could simply be just a metadata change which can be done very fast. In such databases, an exclusive lock is needed only for a very short time for the metadata change. Our understanding is that InnoDB row format does not allow this, and changing row format would be a significant project. Hence we do not consider this approach. Also, note that this approach would not work for operations like defragmentation.
OSC algorithms typically have several phases:
copy - where they make a copy of the table
build - where they work on the copy until the copy is ready with the new schema
replay - where they propagate the changes that happened on the original table to the copy table. This assumes that there is a mechanism for capturing changes.
cut-over - where they switch the tables ie rename the copy table as original. There is typically small amount of downtime while switching the tables. A small amount of replay is also typically needed during the cut-over.
Note that the above operations can be done within the storage engine itself, or using an external (PHP) script. We followed the latter approach as it can be implemented much faster. An advantage of doing within storage engine is some optimizations can be done that are not available while doing these operations externally.
 
Copy Phase
When the binlog is enabled Innodb gets read locks on table S during a statement such as "insert into T select * from S". To avoid this and to reduce the load on MySQL we select the data into an outfile and load from the outfile. 
 
Rather than generating one big outfile, we do multiple scans of the table, where each scan covers about 500000 rows (batchsize is a parameter to the OnlineSchemaChange and default value is set to 500000). The first scan scans from start of the table using LIMIT 500000. Subsequent scans start at the posistion where the previous scan left off. For example, for a 2-column PK, if the 1st select reads till [x, y] the where clause of the 2nd select will look like ((col1 = x and col2 > y) OR (col1 > x)). We patched InnoDB to not get the read locks and expect to get rid of these outfiles in the future. However, we will continue to do multiple scans of table with each scan going after different rows than previous scans.
 
For efficiency, in innodb _plugin 5.1 we drop all non-clustered indexes before loading data into copytable, and recreate them after load. As a future optimization there may be some cases where it is useful to drop and recreate C index as well. We do not drop NC indexes in which 1st column is AUTO_INCREMENT column. Also, in innodb 5.0 we do not drop non-clustered indexes as recreating them is very slow.
 
Capturing changes
Some of the approaches for capturing changes for replay are as follows:
Use statement level binary log. Unfortunately, this approach is problematic when statements that refer to other tables are replayed. If the other tables are read during replay phase, they may return different data than what they returned during the original execution of the statement. If those statements update the other tables, those updates need to be skipped during replay.
Use row level binary log. This approach would work assuming we filter out updates to other tables during replay. However many MySQL deployments don't use row based replication (RBR) yes. Also, we need the original SQL in the binlog even when RBR is used and that feature has yet to appear in an official release.
Use triggers to capture changes. This approach has extra overhead as changes to the table are recorded in a change capture table. Also, we need to get a short duration exclusive lock for creating triggers, as mysql does not allow creating triggers while holding READ lock. If we don't get any lock while creating triggers, we risk losing changes done by transactions that are active at the time of selecting data into outfile, if those changes were done prior to creating triggers. The trigger approach has the advantage of less effort, and less risk of breaking stuff  so we adopt decided to use it for OSC.
The change capture table is referred to as the deltas table. It has has all columns as original table plus two additional columns: an integer autoincrement column to track order of changes AND an integer column to track dml type (insert, update or delete).
An insert trigger is created on the original table to capture all column values of row being inserted in deltas.
A delete trigger is created on original table to capture only the PK columns of row being deleted in deltas.
An update trigger is created on the original table so that if the update does not change the PK columns then it captures new values of all columns in deltas. If the update changes the PK columns, then the update trigger captures it as a delete followed by an insert.  A possible optimization in the future is to log only changed columns.
Replay phase
It is useful to do most of the replay work without blocking changes to the original table. Mutliple replay passes are used and only the final replay is done under WRITE lock on the table. Because there are multiple passes we need to avoid replaying the same change multiple times. The following approaches are available to do this:
Delete the records from deltas as they are replayed. When a record is deleted, the entire record is put in transaction log (possibly containing large columns) and this might be too much load.
Have a column 'done' in deltas and set it for the records as they are replayed. Updates generate less transaction log than delete, but if the 'done' column is not indexed, we will be scanning deltas on each pass anyway.
save IDs of the replayed records in a temporary table so that OSC does not write to deltas.
We choose to save IDs in a temporary table.
 
Another consideration is how to propagate changes from deltas table to the copytable. There are at least two approaches:
select the columns from deltas table into PHP code and pass them back to mysql through update or insert or delete commands. This could move large column values back and forth between PHP and mysql.
Only fetch the ID column in deltas to PHP code, and then construct the insert, update or delete statements such that column values are directly copied from deltas to copytable.
We use the to only feetch the ID columns.
 
There are three phases for replaying changes: after all outfiles are loaded, after indexes are recreated and during the swap phase.
 
Cut-over phase
Mysql offers two ways of renaming a table foo to bar.
Rename table 'foo' to 'bar'. Multiple tables can be renamed atomically using rename command, which makes it attractive for swapping two tables. Unfortunately, this command cannot be executed while holding table locks, or inside a larger transaction (i.e rename has to be transaction all by itself). So we are unable to use this.
Alter table 'foo' rename 'bar'. Alter table causes an implicit commit, but it can be last statement in a multi-statement transaction, and it can be executed while holding table locks. So we use this, but two tables cannot be swapped atomically using alter table command. We need to use two separate statements.
Our cut-over phase looks like
lock tables (original table, new table, change capture table) in exclusive mode
replay any additional changes that happened after last replay
alter original table by renaming it as some old table
alter copytable by renaming it as original table.
Since alter table causes an implicit commit in innodb, innodb locks get released after the first alter table. So any transaction that sneaks in after the first alter table and before the second alter table gets a 'table not found' error. The second alter table is expected to be very fast though because copytable is not visible to other transactions and so there is no need to wait.
 
Error handling
There are two basic cases of errors:
Sql command issued by OSC fails due to some error, but mysql server is still up
Mysql server crashes during OSC
Here are the various entities created by OSC:
triggers
new non-temporary tables (copy table, deltas table, backup table to which the original table is renamed)
temp tables
outfiles
As we create an entity, we use a variable to track its cleanup. For example, when we create deltas, we set a variable $this->cleanup_deltastable to indicate that deltas needs to be cleaned up. This is not necessary for temp tables as they are automatically nuked when the script ends. A cleanup() method does the cleanup based on these cleanup variables. The cleanup() method is used during both successful termination of the script as well as failure termination.
 
However if mysql server crashes, cleanup steps would also fail. The plan to handle mysql failures is to have a mode 'force_cleanup' for the OSC script, which would cleanup all the triggers, non-temporary tables, and outfiles that would have been created by OSC.  One caution while using the force_cleanup mode is if the names of triggers/outfiles/tables that OSC would have created coincide with an existing trigger/outfile/table that has nothing to do with OSC, that entity may get dropped. The chances of coincidence are very slim though as we use prefixes like __osc_ for entities that OSC creates. This issue does not arise during regular cleanup (i.e non-forced) because cleanup is done based on cleanup variables in that case.
 
Note that normally the failures during OSC don't have to be acted on urgently, as the existence of stray tables/outfiles/triggers is not a critical problem. However, an exception is if failure happens after the original table is renamed to a backup table but before copy table is renamed as original table. In that case there should be two tables - backup table and copytable with identical contents except for the schema change. Applications would get 'table not found' errors until the issue is fixed. During force_cleanup, if it detects that both backup table and copytable exist, it renames backup table to original table.
 
Replication
OSC is is not really making any changes on its own, but only propagating the changes done by other transactions (which are replicated). So we set sql_log_bin = 0 for OSC. For schema changes like adding a column, this puts a requirement that the schema change must be done on slaves first. 
 
Assumptions that are validated in the code
The original table must have PK. Otherwise an error is returned.
No foreign keys should exist. Otherwise an error is returned.
No AFTER_{INSERT/UPDATE/DELETE} triggers must exist. Otherwise create trigger would fail and error is returned.
If PK is being altered, post alter table should support efficient lookups on old PK columns. Otherwise an error is returned. The reason for this assumption is that PHP code may have queries/inserts/updates/deletes based on old PK columns and they need to be effiicient. Another reason is during replay, the 'where' clauses generated have old PK columns and so replay phase would be very slow.
If two OSCs are executed on same table concurrently, only the first one to create copytable would succeed and the other one would return an error.
OSC creates triggers/tables/outfiles with prefix __osc_. If coincidentally objects with those names already exist, an error is returned as object creation would fail.
Since we only tested OSC on 5.1.47 and 5.0.84, if it is not one of those two versions, it returns error.
 
<p>Assumptions that are NOT validated in the code</p>
Schema changes are done on slave before master. (If master has more columns than slave, replication may break. )
If OSC is done concurrently with alter table on the same table, race condition could cause "lost schema changes". For example if column foo is being added using OSC and column bar is being added using alter table directly, it is possible that one of the column additions is lost.
Schema changes are backward compatible, such as addition of a column. Column name changes or dropping a column would cause error on the 1st load.
When OSC is run with OSC_FLAGS_FORCE_CLEANUP, it drops triggers/tables/outfiles with prefix __osc_. So if coincidentally objects with those names exist that have nothing to do with OSC, they would get dropped.
 
<p>Steps in detail (listed in the order of execution)</p>
Initialization
create_copy_table
alter_copy_table
create_deltas_table
create_triggers
start snapshot xact
select_table_into_outfile
drop NC indexes
load copy table
Replay Changes
recreate NC indexes
Replay Changes
Swap tables
Cleanup
They are described in more detail below.
 
Slight difference in the sequence of steps in 5.0 and 5.1
Note that (unfortunately) we need to use slightly different sequences for 5.0 and 5.1 – and that is not good. This must be done to compensate for different behavior in those versions.
 
This order works in 5.1 but not 5.0 (I am only showing the relevant part of the sequence):
Lock table in WRITE mode
Create insert, update, delete triggers
Unlock tables.
Start snapshot transaction
Scan deltas table and track these deltas in ‘changes to exclude from replay’
Scan original table into multiple outfiles
End snapshot xact
Load data from all outfiles to copytable
Replay changes that have not been excluded in step 5.
Since the scan done in step 6 should already see the changes captured in step 5, we exclude them from replay.
The above order does not work for 5.0 because creating trigger after locking table hangs in 5.0. See bug 46780.
 
This order works in 5.0 but not in 5.1
Same as above except that 1 and 2 are reversed i.e create triggers before locking.
 
Note that the table lock is for ensuring that transactions that changed the table before triggers were created are all committed. Any changes done after snapshot transaction began in step 4 should be captured by triggers. So even if we get table lock after creating triggers, the purpose of waiting for all prior transactions would still be achieved. So it should work in theory.
 
However, this sequence does not work in 5.1 in my automated unit tests as it causes the scan in step 5 to exclude some changes from replay that are not captured in scan in step 6. (For example, if a concurrent xact updates row R, the snapshot xact step 5 is seeing the row in deltas table inserted by the update, but step 6 is seeing old image of row instead of new image.)
 
MySQL docs state
For transactional tables, failure of a statement should cause rollback of all changes performed by the statement. Failure of a trigger causes the statement to fail, so trigger failure also causes rollback.
 
So that means trigger is executed as part of same transaction as the DML that activated the trigger, right?  We don't know why the snapshot xact in OSC is seeing the affect of trigger but not the affect of original DML and are not sure if this is a bug.
 
Code Vocabulary/Glossary
$this->tablename is name of original table (i.e table being altered)
$this->dbname is name of database
$this->newtablename is name of copy table or new table
$this->deltastable is name of [deltas] table
$this->renametable is name to which the original table is renamed to before discarding
$this->columns, $this->pkcolumns, $this->nonpkcolumns are comma separated lists of all columns, just pk columns and just non PK columns respectively of the original table
$this->newcolumns and $this->oldcolumns are comma separated lists of columns of the original table prefixed by 'NEW.' and 'OLD.' respectively. Similarly we also have $this->oldpkcolumns and $this->newpkcolumns.
IDCOLNAME and DMLCOLNAME are names of ID column and DML TYPE column in [deltas] table
TEMP_TABLE_IDS_TO_EXCLUDE refers to temp table used for IDs to exclude. Its actual name is '__osc_temp_ids_to_exclude'.
TEMP_TABLE_IDS_TO_INCLUDE refers to temp table used for IDs to include. Its actual name is '__osc_temp_ids_to_include'.
$this->insert_trigger, $this->delete_trigger, and $this->update_trigger refer to trigger names.
$this->get_pkmatch_condition($tableA, $tableB) generates condition of the form tableA.pkcolumn1=tableB.pkcolumn1 AND tableA.pkcolumn2=tableB.pkcolumn2 ... where pkcolumn1, pkcolumn2 etc are PK columns of original table. tableA and tableB would be table references in the FROM clause.
Initialization
Here we turn off bin log using 'SET sql_log_bin = 0'.
We do validations like checking for no foreign keys, checking that PK exists, and innodb version.
We also retrieve all column information of the table being altered, so that we don't have to read from information schema multiple times. (QUESTION: what happens if columns get changed by another alter table running in parallel? For now we assume that OPs is aware of alter table commands being run and won't run two in parallel.)
This query retrieves column names.
    $query = "select column_name, column_key from ".
                       "information_schema.columns ".
                       "where table_name ='%s' and table_schema='%s'";
    $query = sprintf($query, $this->tablename,
                                  $this->dbname);
if column_key is not 'PRI', we infer that it is NOT part of primary key.
    // for PK columns we need them to be in correct order as well.
    $query = "select * from information_schema.statistics ".
                      "where table_name = '%s' and TABLE_SCHEMA = '%s' ".
                      " and INDEX_NAME = 'PRIMARY' ".
                      "order by INDEX_NAME, SEQ_IN_INDEX";
    $query = sprintf($query, $this->tablename, $this->dbname);
create_copy_table
copy table is named as concatenate( '__osc_new_', originaltablename) truncated to 64 characters (maxlen). This is done by 'create table <copytable> LIKE <originaltable>'.
 
alter_copy_table
 
DDL command to alter original table is given as input. We replace original table name by copy table name by doing:
     $altercopy = preg_replace('/alter\s+table\s+/i',
                                                    'ALTER TABLE ', $this->altercmd,
                                                    -1, $count);
     $altercopy = preg_replace('/ALTER\s+TABLE\s+
                                                     '.$this->tablename.'/',
                                                     'ALTER TABLE '.
                                                     $this->newtablename,
                                                     $altercopy, -1, $count);
The command is then run to alter copytable in the same way as we want original table to look like after doing alter. If we have < 1 or > 1 matches in either of preg_replace mentioned above, exception is raised.
 
Now we also retrieve index info using the following query so that we can drop and recreate NC indexes. (QUESTION : what happens if a concurrent alter table adds or drops index while this is running? For now we assume that operations is aware of alter table commands being run and won't run two in parallel.
    $query = "select * from information_schema.statistics ".
                      "where table_name = '%s' and ".
                      "TABLE_SCHEMA = '%s' ".
                      "order by INDEX_NAME, SEQ_IN_INDEX";
    $query = sprintf($query, $this->newtablename,
                                  $this->dbname);
The following columns in select list are used:
NON_UNIQUE column: gives info on whether the index is non-unique.
COLUMN_NAME gives the name of the column that is in the index.
SUB_PART column indicates if index is on on part of column. (For example if an index is created on a varchar(1000) column, Innodb only creates index on first 767 chars. SUB_PART column gives this value.)
INDEX_NAME gives the name of index. if name is 'PRIMARY' it is inferred to be primary index.
We also check if old PK (available in $this->pkcolumnarry) is a prefix of atleast one index after the alter table. Note that if old PK is (a, b) and after alter table there is an index on (b, a), that is OK as it supports efficient lookups if values of both a and b are provided. This check is done because replay would be very inefficient if lookup based on old PK columns is inefficient after the alter table.
 
create_deltas_table
creates change capture table. It is named as concatenate('__osc_deltas_', originaltablename) truncated to 64 characters (maxlen). created using:
    $createtable = 'create table %s'. '(%s INT AUTO_INCREMENT, '.
                               '%s INT, primary key(%s)) '.
                               'as (select %s from %s LIMIT 0)';
    $createtable = sprintf($createtable, $this->deltastable,
                                            IDCOLNAME, DMLCOLNAME,
                                            IDCOLNAME, $this->columns,
                                            $this->tablename);
create_triggers
As mentioned before, in 5.1 we lock table and create triggers and then unlock table, but in 5.0, we create the triggers and then lock the table and unlock it.
 
Insert trigger is created as:
    $trigger = 'create trigger %s AFTER INSERT ON %s'.
                        'FOR EACH ROW '.
                        'insert into %s(%s, %s) '. 'values (%d, %s)';
    $trigger = sprintf($trigger, $this->insert_trigger,
                                     $this->tablename,
                                     $this->deltastable, DMLCOLNAME,
                                     $this->columns, DMLTYPE_INSERT,
                                     $this->newcolumns);
Delete trigger is created as
    $trigger = 'create trigger %s AFTER DELETE ON'.
                        '%s FOR EACH ROW '.
                        'insert into %s(%s, %s) '. 'values (%d, %s)';
    $trigger = sprintf($trigger, $this->delete_trigger,
                                     $this->tablename,
                                     $this->deltastable, DMLCOLNAME,
                                     $this->pkcolumns, DMLTYPE_DELETE,
                                     $this->oldpkcolumns);
Update trigger is created as
    // if primary key is updated, map the update
    // to delete followed by insert
    $trigger = 'create trigger %s AFTER UPDATE ON'.
                        '%s FOR EACH ROW '.
                        'IF (%s) THEN '. ' insert into %s(%s, %s) '.
                        ' values(%d, %s); '.
                        'ELSE '. ' insert into %s(%s, %s) '.
                        ' values(%d, %s), '. ' (%d, %s); '. 'END IF';
     $trigger = sprintf($trigger, $this->update_trigger,
                                      $this->tablename,  
                                      $this->get_pkmatch_condition('NEW', 'OLD'),
                                      $this->deltastable, DMLCOLNAME,
                                      $this->columns,
                                      DMLTYPE_UPDATE, $this->newcolumns,
                                      $this->deltastable, DMLCOLNAME,
                                      $this->columns, DMLTYPE_DELETE,
                                      $this->oldcolumns,
                                      DMLTYPE_INSERT, $this->newcolumns);
start snapshot xact
Here we 'start transaction with consistent snapshot'.
 
At this point the deltas table may already have some changes done by transactions that have committed before out snapshot began. Since such changes are already reflected in our snapshot, we don't want to replay those changes again during replay phase. So we also create a temp table named __osc_temp_ids_to_exclude to save the IDs of records that already exist in deltas table.
    $createtemp = 'create temporary table %s(%s INT, %s'.
                                'INT, primary key(%s))';
    $createtemp = sprintf($createtemp, $temptable,
                                             IDCOLNAME,
                                             DMLCOLNAME, IDCOLNAME);
Since innodb gets read locks during "insert into T1 select * from T2" state   ments, we select out into outfile and load from that. Outfile is created in 'secure-file-priv' folder with name concatenate('__osc_ex_', $this->tablename).
    $selectinto = "select %s, %s from %s ".
                             "order by %s into outfile '%s' ";
    $selectinto = sprintf($selectinto, IDCOLNAME,
                                         DMLCOLNAME, $this->deltastable,
                                         IDCOLNAME, $outfile);
    // read from outfile above into the temp table
    $loadsql = sprintf("LOAD DATA INFILE '%s' INTO'.
                                     'TABLE %s(%s, %s)",
                                     $outfile, $temptable,
                                     IDCOLNAME, DMLCOLNAME);
select_table_into_outfile
If an outfile folder is passed in, we use that. Otherwise, if @@secure_file_priv is non-NULL, we use it as outfile folder. Otherwise we use @@datadir/dbname as outfile folder. We assume @@datadir is non-NULL.
 
Outfile is named as concatenate('__osc_tbl_', originaltablename'); Since we use multiple outfiles, they are suffixed .1,.2,.3 etc.
We also commit snapshot xact here.
 
drop NC indexes
In 5.1 we iterate over the index info gathered in previous step and drop all indexes whose name is NOT 'PRIMARY'. We also don't drop indexes in which first column is AUTO_INCREMENT column. We use this command to drop index:
$drop = sprintf('drop index %s on %s', $this->indexname, $this->newtablename);
Indexes are not dropped in 5.0 as mentioned before.
 
load copy table
We use this command to load each outfile:
$loadsql = sprintf("LOAD DATA INFILE '%s' INTO".
                                 "TABLE %s(%s)",
                                  $this->outfile_table,
                                 $this->newtablename,
                                  $this->columns);
recreate NC indexes
We iterate over the index info gathered in 'alter_copy_table' step and recreate all indexes whose name is NOT 'PRIMARY'.
We use one alter table command to create all NC indexes.
 
If the 'SUB_PART' column value in information_schema.statistics is not-null we use it while building columnlist. For example, if SUB_PART value for column 'comment' is 767, we use 'comment(767)' in the columnlist passed to create index command.
 
Replay changes
 
As mentioned before replay changes could be done multiple times. We maintain a temp table called TEMP_TABLE_IDS_TO_EXCLUDE to track those IDs that have been processed already. The set of IDs to process is obtained by taking the IDs from deltas table and excluding those that are in TEMP_TABLE_IDS_TO_EXCLUDE and is saved in TEMP_TABLE_IDS_TO_INCLUDE.
// Select from deltastable that are not in
// TEMP_TABLE_IDS_TO_EXCLUDE.
// Use left outer join rather than 'in' subquery for better perf.
$idcol = $this->deltastable.'.'.self::IDCOLNAME;
$dmlcol = $this->deltastable.'.'.self::DMLCOLNAME;
$idcol2 = self::TEMP_TABLE_IDS_TO_EXCLUDE.'.'.self::IDCOLNAME;
$selectinto = "select %s, %s ". "from %s LEFT JOIN %s ON %s = %s ".
                         "where %s is null order by %s into outfile '%s' ";
$selectinto = sprintf($selectinto, $idcol, $dmlcol,
                                      $this->deltastable,
                                      self::TEMP_TABLE_IDS_TO_EXCLUDE,
                                       $idcol,
                                      $idcol2, $idcol2, $idcol, $outfile);
We process about 500 rows in a transaction (except for the final replay which happens while holding WRITE lock on table, which is done without starting any new transaction).
 
Here is the query to retrieve IDs and dml type from TEMP_TABLE_IDS_TO_INCLUDE.
$query = sprintf('select %s, %s from %s order by %s',
                               IDCOLNAME, DMLCOLNAME,
                               TEMP_TABLE_IDS_TO_INCLUDE,
                                IDCOLNAME);
DMLCOLNAME column tells if it is insert, delete or update.
 
Here is how insert is replayed:
$insert = sprintf('insert into %s(%s) select %s'.
                              'from %s where %s.%s = %d',
                              $this->newtablename, 
                              $this->columns,
                              $this->columns,
                              $this->deltastable,
                              $this->deltastable,
                              IDCOLNAME,
                              $row[IDCOLNAME]);
Here is how delete is replayed:
$delete = sprintf('delete %s from %s, %s '.
                               'where %s.%s = %d AND %s',
                               $newtable, $newtable,
                               $deltas, $deltas,
                               IDCOLNAME,
                               $row[IDCOLNAME],
                               $this->get_pkmatch_condition($newtable,
                                                                                        $deltas));
Here is how update is replayed:
$update = sprintf('update %s, %s SET %s where '.
                                 '%s.%s = %d AND %s ',
                                 $newtable, $deltas,
                                 $assignment, $deltas,
                                 IDCOLNAME,
                                 $row[IDCOLNAME],
                                 $this->get_pkmatch_condition($newtable,
                                  $deltas));
Swap tables
 
Here are the steps as mentioned in cut-over phase:
TURN AUTOCOMMIT OFF: 'set session autocommit=0' // without this lock tables is not getting innodb lock
lock all tables in WRITE mode: 
$lock = sprintf('lock table %s WRITE, %s WRITE, %s WRITE', $this->tablename,
                          $this->newtablename, $this->deltastable);
final replay
              $rename_original = sprintf('alter table %s rename %s', 
                                                              $this->tablename, $this->renametable);
              $rename_new = sprintf('alter table %s rename %s', 
                                                         $this->newtablename, $this->tablename);
COMMIT // alter tables would have already caused implicit commits in innodb
unlock tables
TURN AUTOCOMMIT ON: 'set session autocommit=1'
Cleanup
ROLLBACK in case we are in the middle of a xact
Turn on autocommit in case we turned it off
if trigger cleanup variables are set, drop triggers and unset trigger cleanup variables
if outfile cleanup variables are set, delete the outfiles and unset outfile cleanup variables
if cleanup variable is set for both newtable and renamedtable, then it means failure happened between the two alter tables. In this case just rename renamedtable as original table, and unset cleanup variable for renamedtable.
if cleanup variable is set for newtable, renamedtable or deltas table, drop the corresponding tables, and unset corresponding cleanup variable
In the force cleanup mode we will pretend as though all cleanup variables are set, and use 'drop if exists'.