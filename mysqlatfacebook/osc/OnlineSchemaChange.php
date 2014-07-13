<?php
// Copyright 2004-present Facebook. All Rights Reserved.

/*
Copyright 2010 Facebook. All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

  1. Redistributions of source code must retain the above copyright notice, this
     list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright notice,
     this list of conditions and the following disclaimer in the documentation
     and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY Facebook ``AS IS'' AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
EVENT SHALL <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those
of the authors and should not be interpreted as representing official policies,
either expressed or implied, of Facebook.

The following sections in the code have been taken from
http://code.google.com/p/openarkkit/ authored by Shlomi Noach and adapted.
1. Splitting the scan of the original table into multiple scans based on PK
   ranges. Refer methods initRangeVariables(), refreshRangeStart(),
   assignRangeEndVariables(), getRangeStartCondition().
The code taken from http://code.google.com/p/openarkkit/ comes with the
following license:
Copyright (c) 2008-2009, Shlomi Noach
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.
* Neither the name of the organization nor the names of its contributors may be
  used to endorse or promote products derived from this software without
  specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

/**
 * OnlineSchemaChange class encapsulates all the steps needed to do online
 * schema changes. Only public functions by category are
 *  __construct(), execute(), forceCleanup()
 * static methods getOscLock(), releaseOscLock(), isOscLockHeld()
 * static methods getCleanupTables(), serverCleanup() and dropTable().
 *
 * execute() returns true/false indicating success or failure. In rare
 * cases where an exception occurs while cleaning up, it may raise an exception.
 * In that case, caller needs to run OSC with OSC_FLAGS_FORCE_CLEANUP to clean
 * up triggers/tables/files created by OSC (calling forceCleanup() method would
 * run OSC with this flag).
 *
 * IMPORTANT ASSUMPTION:
 * Caller needs to check that there are no foreign keys to/from table being
 * altered and that there are no triggers already defined on the table.
 *
 * @author: nponnekanti with some parts authored by Shlomi Noach as noted
 * in the license section above.
 */

// Default value for flags is 0.
//
// OSC_FLAGS_CHECKSUM causes checksum comparison before switching tables.
// Checksum is compared while holding table lock. ALso, it only makes sense
// in some cases. For example, when adding new column, checksum will NOT
// match by design. However when adding index on existing column, or when
// rebuilding table without schema changes, checksum is expected to match.
//
// OSC_FLAGS_ERRORTEST causes exceptions to be raised randomly when executing
// a SQL statement in OSC. Useful for error testing.
//
// OSC_FLAGS_FORCE_CLEANUP causes ONLY cleanup to be done, i.e no schema
// change. It cleans up triggers/tables/outfiles left over from prior OSC
// run. It treats errors as warnings and proceeds forward rather than stopping
// on 1st error.

// drops renamed original table
define('OSC_FLAGS_DROPTABLE',       0x00000001);
// delete log files as well
define('OSC_FLAGS_DELETELOG',       0x00000002);
// See above.
define('OSC_FLAGS_FORCE_CLEANUP',   0x00000004);
// don't do cleanup (helps testing)
define('OSC_FLAGS_NOCLEANUP',       0x00000008);
// see note above on this flag
define('OSC_FLAGS_CHECKSUM',        0x00000010);
// bypasses some checks on new PK
define('OSC_FLAGS_ACCEPTPK',        0x00000020);
// Allow a column to be dropped
define('OSC_FLAGS_DROPCOLUMN',      0x00000040);
// Turn off logging except errors
define('OSC_FLAGS_NOLOG',           0x00000080);
// Turn off verbose logging
define('OSC_FLAGS_COMPACTLOG',      0x00000100);
// DON'T USE THIS IN PROD.
define('OSC_FLAGS_ERRORTEST',       0x00000200);
// bypasses version check
define('OSC_FLAGS_ACCEPT_VERSION',  0x00000400);
// use upto 54 char prefix of table name in trigger/table names
define('OSC_FLAGS_NAMEPREFIX',      0x00000800);
// run OSC ignoring long xact
define('OSC_FLAGS_LONG_XACT_OK',    0x00001000);
// disable drop/recreate index(5.1)
define('OSC_FLAGS_NOREINDEX',       0x00002000);
// forces usage of newly added PK
define('OSC_FLAGS_USE_NEW_PK',      0x00004000);
// when adding PK or UNIQUE, delete duplicates
define('OSC_FLAGS_ELIMINATE_DUPS',  0x00008000);
// log to stdout in addition to log files
define('OSC_FLAGS_LOG_STDOUT',      0x00010000);


// useful for enclosing column names, index names and table names in quotes
function quotify($name) {
  return '`'.$name.'`';
}

class IndexColumnInfo {
  public $name;
  public $prefix; // used when index is on prefix of a column

  // Note that ascending/descending is not currently supported.
  public function __construct($input_name, $input_prefix) {
    $this->name = $input_name;
    $this->prefix = $input_prefix;
  }
}

class IndexInfo {

  // No index type because innodb only allows Btree.
  // Also note that spatial and fulltext indexes are not available in Innodb.
  public function __construct($input_tablename, $input_indexname,
                              $input_non_unique, $input_is_auto_increment) {
    $this->tablename = $input_tablename;
    $this->indexname = $input_indexname;
    $this->nonUnique = $input_non_unique;
    $this->isAutoIncrement = $input_is_auto_increment;
    $this->columns = array();
  }


  public function addColumn($input_name, $input_prefix = '') {
    $column = new IndexColumnInfo($input_name, $input_prefix);
    $this->columns[] = $column;
  }

  public function getCreateSql() {

    $columnlist = '';

    $comma = ''; // no comma first time
    foreach ($this->columns as $column) {
      $prefix = ($column->prefix == '' ? '' : sprintf('(%d)', $column->prefix));
      $columnlist .= $comma.$column->name.$prefix;
      $comma = ',';
    }

    $unique = ($this->nonUnique ? '' : 'unique');
    $create = sprintf(' ADD %s index %s (%s) ',
                      $unique, $this->indexname, $columnlist);
    return $create;
  }

  public function isPrimary() {
    return ($this->indexname === quotify('PRIMARY'));
  }

  public function getDropSql() {
    $drop = sprintf('drop index %s on %s', $this->indexname, $this->tablename);
    return $drop;
  }


}

class OnlineSchemaChange {

  const IDCOLNAME = '_osc_ID_';
  const DMLCOLNAME =  '_osc_dml_type_';

  // Note that an update that affects PK is mapped as delete followed by insert
  const DMLTYPE_INSERT = 1;
  const DMLTYPE_DELETE = 2;
  const DMLTYPE_UPDATE = 3;

  const TEMP_TABLE_IDS_TO_EXCLUDE = '__osc_temp_ids_to_exclude';
  const TEMP_TABLE_IDS_TO_INCLUDE = '__osc_temp_ids_to_include';

  // we only retry on timeout or deadlock error
  const LOCK_MAX_ATTEMPTS = 3;

  // Names (such as table name etc) can be maxlen of 64.
  const LIMITNAME = 64;
  // A value that is at least LIMITNAME + length of prefix that OSC adds when
  // generating names for triggers and tables.
  const NOLIMIT = 100;

  // if set, log success cases only when verbose logging is enabled (i.e
  // OSC_FLAGS_COMPACTLOG is OFF). By default success cases are logged
  // unless OSC_FLAGS_NOLOG is set globally.
  const LOGFLAG_VERBOSE = 0x1;

  // if set, treat failure as warning rather than error.
  // By default, failures are treated as errors unless OSC_FLAGS_FORCE_CLEANUP
  // is set globally.
  const LOGFLAG_WARNING = 0x2;

  // when OSC_FLAGS_ERRORTEST is set, raise errors randomly for 1 in 15 cmds
  const TEST_ERROR_ODDS = 15;

  // the string that OSC gets lock on
  const LOCK_STRING = "OnlineSchemaChange";

  // A static method that can be used by other scripts to check if OSC
  // lock is held.
  // Returns connection id of the connection that holds OSC lock if any.
  public static function isOscLockHeld($conn) {
    $lock_command = sprintf("select is_used_lock('%s') as osc_connection_id",
                            self::LOCK_STRING);
    $lock_result  = mysql_query($lock_command, $conn);
    if (!$lock_result) {
      Throw new Exception("Failed :".$lock_command.mysql_error($conn));
    }

    $row = mysql_fetch_assoc($lock_result);

    if (!$row) {
      Throw new Exception("Returned no rows:".$lock_command);
    }

    return $row['osc_connection_id'];
  }

  // Returns array of (dbname, tablename) arrays for which OSC cleanup may be
  // needed. Basically looks for triggers/tables that may have been left behind
  // by OSC. It does not look for outfiles though as the main use case is
  // to cleanup tables/triggers that may have been inadvertantly captured in a
  // db backup, and hence to restored database.
  public static function getCleanupTables($conn) {
    $q1 = "(select T.table_schema as db, substr(T.table_name, 11) as obj ".
          " from information_schema.tables T ".
          " where T.table_name like '__osc_%')";
    $q2 = "(select T.trigger_schema as db, substr(T.trigger_name, 11) as obj ".
          " from information_schema.triggers T ".
          " where T.trigger_name like '__osc_%')";
    $q = $q1." union distinct ".$q2." order by db, obj";

    $result  = mysql_query($q, $conn);
    if (!$result) {
      Throw new Exception("Failed :".$q.mysql_error($conn));
    }

    $db_and_tables = array();
    while ($row = mysql_fetch_assoc($result)) {
      $db_and_tables[] = array('db' => $row['db'],
                               'table' => $row['obj']);
    }

    return $db_and_tables;
  }

  // Connects to the server identified by $sock, $user, $password and cleans up
  // any left over tables/triggers left over by OSC in any database.
  // Main use case is as follows:
  // If a db is backed up inadvertantly while OSC is running, it may have some
  // OSC tables/triggers in it and there is a need to remove them.
  //
  // $flags is the same as for __construct documented above,
  // but the main flags of interest for cleanup are OSC_FLAGS_DELETELOG,
  // OSC_FLAGS_DROPTABLE.
  //
  public static function serverCleanup($sock, $user, $password, $flags=0) {
    $conn = self::openConnectionOnly($sock, $user, $password);
    OnlineSchemaChange::releaseOscLock($conn);

    $db_and_tables = self::getCleanupTables($conn);

    foreach ($db_and_tables as $db_and_table) {
      $db = $db_and_table['db'];
      $table = $db_and_table['table'];
      $ddl = ''; // empty alter table command as we don't intend to alter
      $osc = new OnlineSchemaChange($sock, $user, $password, $db, $table, $ddl,
                                    null, OSC_FLAGS_FORCE_CLEANUP|$flags);

      $osc->forceCleanup();
    }

  }


  /**
   $input_sock              -  socket to use
   $input_user              -  username to use to connect
   $input_password          -  password to use to connect
   $input_dbname            -  database name
   $input_tablename         -  table being altered
   $input_createcmd         -  alter table DDL. See below.
   $input_outfile_folder    -  folder for storing outfiles. See below.
   $input_flags             -  various flags described below
   $input_batchsize_load    -  batchsize to use when selecting from table to
                               outfiles. Each outfile generated (except last
                               one) will have this many rows.
   $input_batchsize_replay  -  transaction size to use during replay phase.
                               Commit after this many single row
                               insert/update/delete commands are replayed.
   $input_long_xact_time    -  If OSC finds a long running xact running for
                               this many seconds, it bails out unless
                               OSC_FLAGS_LONG_XACT_OK is set.
   $input_logfile_folder    -  folder for storing logfiles. See below.
   $input_linkdir           -  symlimk support. End with /. See below.


   $input_createcmd:
   OSC works by making a copy of the table, doing schema change on the table,
   replaying changes and swapping the tables. While this input_createcmd
   would create the original table, OSC needs to modify to to affect the
   copytable.

   It first replaces 'create table ' (case insensitive and with possible
   multiple spaces before and after table) with 'CREATE TABLE ' (upper case and
   with exactly one space before and after TABLE).

   Then it replaces 'CREATE TABLE <originaltable>' with
   'CREATE TABLE <copytable>'. This is case sensitive replace since table names
   are case sensitive.

   While doing the above replaces, if there is no match or > 1 matches, it
   raises exception. (So if u have comments etc which contain 'create table' or
   'create table <tablename>', it may find > 1 match and raise exception.

   $input_outfile_folder (end with /):
   If a folder name is supplied it is used. The folder must exist. Otherwise
   invalid outfile folder exception is raised. Otherwise, if @@secure_file_priv
   is non-null, it is used. Otherwise @@datadir/<dbname> folder is used. (It is
   assumed that @@datadir is not NULL.)

   $input_logfile_folder (end with /):
   Used for storing osc log files.

   $input_linkdir (end with /)
   This can be used to override the default behavior with respect to symlinks.
   The default behavior is as follows:
   (a) If the table is not a symlink (i.e is in @@datadir/<dbname>) then the
       altered table will also not be a symlink.
   (b) If the table is a symlink and actually lives in folder foo, then the
       altered table will also be put in same folder.
   input_linkdir can be specified to override the behavior as follows:
   (a) To move a table from @@datadir/<dbname> to input_linkdir/<dbname>
   (b) To move a table from its current location to @@datadir/<dbname>
       specify linkdir to be same as @@datadir.

  */
  public function __construct($input_sock = '',
                              $input_user,
                              $input_password,
                              $input_dbname,
                              $input_tablename,
                              $input_createcmd,
                              $input_outfile_folder = null,
                              $input_flags = 0,
                              $input_batchsize_load = 500000,
                              $input_batchsize_replay = 500,
                              $input_long_xact_time = 30,
                              $input_backup_user = "backup",
                              $input_logfile_folder = "/var/tmp/",
                              $input_linkdir = null) {

    $this->dbname = trim($input_dbname, '`'); // remove quotes if present
    $this->qdbnameq = quotify($this->dbname); // quoted dbname
    $this->tablename = trim($input_tablename, '`'); // remove quotes if present
    $this->qtablenameq = quotify($this->tablename); // quoted tablename
    $this->flags = $input_flags;
    $this->batchsizeLoad = $input_batchsize_load;
    $this->batchsizeReplay = $input_batchsize_replay;
    $this->outfileFolder = $input_outfile_folder;
    $this->logfileFolder = $input_logfile_folder;
    $this->backup_user = $input_backup_user;
    $this->longXactTime = $input_long_xact_time;
    $this->symlinkDir = $input_linkdir;
    if ($this->symlinkDir) {
      // remove spaces
      $this->symlinkDir = trim($this->symlinkDir);
      // ensure it ends with / but not two //
      $this->symlinkDir = rtrim($this->symlinkDir, '/').'/';
    }

    $this->createcmd = $input_createcmd;

    // note that this sets up log files. So any errors raised
    // before this is done won't be logged.
    $this->initLogFiles();

    // set to IGNORE or empty to add to queries which manipulate the table
    $this->ignoredups = $input_flags & OSC_FLAGS_ELIMINATE_DUPS ? 'IGNORE' : '';

    // In all the tables/triggers that OSC creates, keep the tablename
    // starting at exactly 11th character so that it is easy to get the
    // original tablename from the object (i.e prefix is 10 chars).
    // Mysql allows only 64 characters in names. Adding prefix can make
    // it longer and cause failure in mysql. Let it fail by default. If
    // caller has set OSC_FLAGS_NAMEPREFIX, then use prefix of tablename.
    // However that makes it impossible to construct original tablename
    // from the name of the object. So methods like getCleanupTables
    // may not correctly return tablenames.

    $limit = (($this->flags & OSC_FLAGS_NAMEPREFIX) ?
              self::LIMITNAME :
              self::NOLIMIT);

    // table to capture deltas
    $this->deltastable = substr('__osc_chg_'.$this->tablename, 0, $limit);


    // trigger names for insert, delete and update
    $this->insertTrigger = substr('__osc_ins_'.$this->tablename, 0, $limit);
    $this->deleteTrigger = substr('__osc_del_'.$this->tablename, 0, $limit);
    $this->updateTrigger = substr('__osc_upd_'.$this->tablename, 0, $limit);

    // new table name
    $this->newtablename = substr('__osc_new_'.$this->tablename, 0, $limit);
    $this->renametable = substr('__osc_old_'.$this->tablename, 0, $limit);

    $this->isSlaveStopped = false;

    // open connection as late as possible
    $this->sock = $input_sock;
    $this->user = $input_user;
    $this->password = $input_password;
    $this->conn = $this->openAndInitConnection();
  }

  // this just opens a connection
  protected static function openConnectionOnly($sock, $user, $password) {
    $host_and_sock = 'localhost'.(empty($sock) ? '' : ':'.$sock);
    $conn = mysql_connect($host_and_sock, $user, $password, true);
    if (!$conn) {
      $error = "Could not connect to localhost using given socket/user/pwd:";
      Throw new Exception($error.mysql_error());
    }
    return $conn;
  }

  // this opens connection, switches off binlog, gets OSC lock, does a use db
  protected function openAndInitConnection() {
    $this->conn = $this->openConnectionOnly($this->sock, $this->user,
                                            $this->password);
    $this->turnOffBinlog();
    $this->setSqlMode();

    $this->executeSqlNoResult('Selecting database', 'use '.$this->qdbnameq);

    // get this lock as soon as possible as this lock is used to
    // determine if OSC is running on the server.
    $this->getOscLock($this->conn);

    return $this->conn;
  }

  // Gets OSC lock. Used within OSC, and possibly other scripts
  // to prevent OSC from running. Throws exception if lock cannot
  // be acquired.
  public static function getOscLock($conn) {
    $lock_command = sprintf("select get_lock('%s', 0) as lockstatus",
                            self::LOCK_STRING);
    $lock_result  = mysql_query($lock_command, $conn);
    if (!$lock_result) {
      Throw new Exception("GET_LOCK failed:".mysql_error($conn));
    }

    $row = mysql_fetch_assoc($lock_result);

    if (!$row) {
        Throw new Exception("GET_LOCK returned no rows");
    }

    if ($row['lockstatus'] != 1) {
      Throw new Exception("GET_LOCK returned ".$row['lockstatus']);
    }

  }

  // Releases OSC lock. Does not return anything.
  // Throws exception if release_lock statement fails, such as if connection
  // is not valid. However, if lock was not held, it just silently returns.
  public static function releaseOscLock($conn) {
    $lock_command = sprintf("do release_lock('%s')",
                            self::LOCK_STRING);
    $lock_result  = mysql_query($lock_command, $conn);
    if (!$lock_result) {
      Throw new Exception("RELEASE_LOCK failed:".mysql_error($conn));
    }
    if($pid = OnlineSchemaChange::isOscLockHeld($conn)) {
      $kill_command = sprintf("kill %s", $pid);
      $kill_result  = mysql_query($kill_command, $conn);
      if (!$kill_result) {
        Throw new Exception("RELEASE_LOCK failed:".mysql_error($conn));
      }
    }
  }

  // Connect to the server identified by $sock, $user, $password and drop
  // table specified by by $table. If the table is partitioned we will drop
  // a patition at a time in order to avoid performance issues associated with
  // dropping all partitions at the same time.
  public static function dropTable($tablename, $conn) {
    $show_query = "SHOW CREATE TABLE `$tablename`";
    if( !$show_result = @mysql_query($show_query, $conn)) {
      return;
    }
    $tbl_dfn = mysql_fetch_array($show_result);
    $partitions = array();
    // Cycle through each partition and delete them one at a time
    if (preg_match_all("/PARTITION ([^ ]+) VALUES/", $tbl_dfn[1], $partitions)){
      $partitions = $partitions[1];
      // length(table) - 1 otherwise we leave a paritioned table with no
      // partitions, which MySQL errors on.
      array_pop($partitions);
      foreach ($partitions as $partition) {
        // Intentionally ignoring any issues.
        $drop_query = "ALTER TABLE `$tablename` DROP PARTITION `$partition`";
        mysql_query($drop_query,$conn);
      }
    }
    // Intentionally ignoring any issues. We sometimes call
    // drop table unnecessarily.
    $drop_query = "DROP TABLE `$tablename`";
    @mysql_query($drop_query,$conn);
  }




  protected function raiseException($error_prefix, $sqlfailed = true) {
    $error = $error_prefix.($sqlfailed ? mysql_error($this->conn) : '');
    $errno = ($sqlfailed ? mysql_errno($this->conn) : 0);
    $logtext = sprintf("Exception: errno:-%d, error:-%s\n", $errno, $error);
    $this->logError($logtext);
    Throw new Exception($error, $errno);
  }

  protected function raiseNonSqlException($error_prefix) {
    $this->raiseException($error_prefix, false);
  }

  protected function getSlaveStatus() {
    $query = 'show slave status';

    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Failed to get slave status');
    }

    // if rows are returned, it means we are running on a slave
    if ($row = mysql_fetch_assoc($result)) {
      return (($row['Slave_IO_Running'] == "Yes") &&
              ($row['Slave_SQL_Running'] == "Yes"));
    } else {
      // not configured as slave.
      return false;
    }
  }

  // if slave is running, then stop it
  protected function stopSlave() {
    if ($this->getSlaveStatus()) {
      $this->executeSqlNoResult('stopping slave', 'stop slave');
      $this->isSlaveStopped = true;
    }
  }

  // we start slave only if we stopped it
  protected function startSlave() {
    if ($this->isSlaveStopped) {
      $this->isSlaveStopped = false;
      $this->executeSqlNoResult('starting slave', 'start slave');
    }
  }

  // wrapper around unlink
  protected function executeUnlink($file, $check_if_exists = false) {
    $this->logCompact("--Deleting file:".$file."\n");

    if (($check_if_exists || ($this->flags & OSC_FLAGS_FORCE_CLEANUP)) &&
        !file_exists($file)) {
      return true;
    }

    if(!file_exists($file)){
        $this->logWarning("File ".$file." does not exist\n");
        return false;
    }

    if (!unlink($file)) {
      if ($this->flags & OSC_FLAGS_FORCE_CLEANUP) {
        // log and move on
        $this->logWarning("Could not delete file:".$file."\n");
        return false;
      } else {
        $this->raiseException('Could not delete file:'.$file, false);
      }
    }
    return true;
  }

  protected function executeShellCmd($description, $cmd) {

    $cmd = escapeshellcmd($cmd);
    $output = array();
    $status = 0;
    exec($cmd, $output, $status);

    $this->logCompact("$description cmd=$cmd status = $status\n");
    foreach ($output as $outputline) {
      $this->logVerbose("$description cmd output line : $outputline\n");
    }

    if ($status !== 0) {
      $this->raiseNonSqlException("$description cmd $cmd returned $status\n");
    }

  }

  // wrapper around mysql_query
  // used for sql commands for which we don't have a resultset
  // logflags is used to specify:
  // whether to log always (default) or only in verbose mode (LOGFLAG_VERBOSE)
  // whether failure is error (default) or warning (LOGFLAG_WARNING)
  protected function executeSqlNoResult($sql_description, $sql,
                                      $logflags = 0) {
    $logtext = "--".$sql_description."\n".$sql.";\n";
    if ($logflags & self::LOGFLAG_VERBOSE) {
      $this->logVerbose($logtext) ;
    } else {
      $this->logCompact($logtext);
    }

    if ($this->flags & OSC_FLAGS_ERRORTEST) {
      $odds = self::TEST_ERROR_ODDS;
      // reduce odds for commands that are more frequent as indicated by
      // LOGFLAG_VERBOSE
      $odds = (($logflags & self::LOGFLAG_VERBOSE) ? pow($odds, 4) : $odds);
      $fail = mt_rand(1, $odds);
      if ($fail === $odds) {
        $this->raiseException("Faking error ".$sql_description.":".$sql, false);
      }
    }

    if (!@mysql_query($sql, $this->conn)) {
      $error = mysql_error($this->conn);
      if (($this->flags & OSC_FLAGS_FORCE_CLEANUP) ||
          ($logflags & self::LOGFLAG_WARNING)) {
        // log error and move on
        $this->logWarning("WARNING: SQL :-".$sql.". Error :-".$error."\n");
        return false;
      } else {
        $this->raiseException($sql_description.' failed. SQL:-'.$sql.'.');
      }
    }

    return true;
  }

  protected function turnOffBinlog() {
    $this->executeSqlNoResult('Turning binlog off', 'SET sql_log_bin = 0');
  }

  protected function setSqlMode() {
    $this->executeSqlNoResult('Setting sql_mode to STRICT_ALL_TABLES',
                              'SET sql_mode = STRICT_ALL_TABLES');
  }

  // some header info that is useful to log
  protected function getOSCHeader() {
    $logtext = "--OSC info: time=%s, db=%s, table=%s, flags=%x\n".
               "--CREATE=%s\n";
    $logtext = sprintf($logtext, date('c'), $this->dbname, $this->tablename,
                       $this->flags, $this->createcmd);
    return $logtext;
  }

  // logs stuff only when verbose logging is enabled i.e both
  // OSC_FLAGS_NOLOG and OSC_FLAGS_COMPACTLOG are OFF.
  // Use methods logWarning/logError for warnings/errors.
  protected function logVerbose($logtext) {
    if ($this->flags & (OSC_FLAGS_NOLOG|OSC_FLAGS_COMPACTLOG)) {
      return;
    }
    $this->logCompact($logtext);
  }

  // logs stuff unless OSC_FLAGS_NOLOG is set.
  // Use methods logWarning/logError for warnings/errors.
  protected function logCompact($logtext) {
    if (empty($this->oscLogFP)) {
      echo "Attempt to log before log file pointer setup:".$logtext."\n!";
      return;
    }
    $timestamp = date('Y-m-d G:i:s');

    if ($this->flags & OSC_FLAGS_NOLOG) {
      return;
    }
    fwrite($this->oscLogFP, $timestamp . ' ' . $logtext);

    if ($this->flags & OSC_FLAGS_LOG_STDOUT) {
      print "$timestamp $logtext\n";
    }
  }

  // used for logging warnings
  protected function logWarning($logtext) {
    if (empty($this->oscWarningFP)) {
      echo "Attempt to log before warning file pointer setup:".$logtext."\n!";
      return;
    }

    if ($this->flags & OSC_FLAGS_LOG_STDOUT) {
      print "$logtext\n";
    }

    // since we don't expect many warnings, put header info in every warning
    fwrite($this->oscWarningFP, $this->getOSCHeader());
    fwrite($this->oscWarningFP, $logtext);
  }

  // used for logging errors
  protected function logError($logtext) {
    if (empty($this->oscErrorFP)) {
      echo "Attempt to log before error file pointer setup:".$logtext."\n!";
      return;
    }

    if ($this->flags & OSC_FLAGS_LOG_STDOUT) {
      print "$logtext\n";
    }

    // since we don't expect many errors, put header info in every error
    fwrite($this->oscErrorFP, $this->getOSCHeader());
    fwrite($this->oscErrorFP, $logtext);
  }



  // Retrieves column names of table being altered and stores in array.
  // Stores PK columns, non-PK columns and all columns in separate arrays.
  protected function initColumnNameArrays() {
    $this->columnarray = array();
    $this->pkcolumnarray = array();
    $this->nonpkarray = array();
    $this->nonpkcolumns = '';

    // get list of columns in new table
    $query = "select column_name ".
             "from information_schema.columns ".
             "where table_name ='%s' and table_schema='%s'";
    $query = sprintf($query, $this->newtablename, $this->dbname);
    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Initializing column names failed.'.$query);
    }
    $newcolumns = array();
    while ($row = mysql_fetch_assoc($result)) {
      $newcolumns[] = $row['column_name'];
    }

    $query = "select column_name, column_key, extra ".
             "from information_schema.columns ".
             "where table_name ='%s' and table_schema='%s'";
    $query = sprintf($query, $this->tablename, $this->dbname);

    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Initializing column names failed.'.$query);
    }

    $comma = ''; // no comma first time
    while ($row =  mysql_fetch_assoc($result)) {
      // column must have been dropped from new table, skip it
      if (!in_array($row['column_name'], $newcolumns)) {
        continue;
      }
      $column_name = quotify($row['column_name']);
      $this->columnarray[] = $column_name;
      // there should be atmost one autoincrement column
      if (stripos($row['extra'], 'auto_increment') !== false) {
        if (isset($this->autoIncrement)) {
          $err = sprintf("Two auto_increment cols: %s, %s",
                         $this->autoIncrement, $column_name);
          $this->raiseException($err, false);
        }
        $this->autoIncrement = $column_name;
      }
      if ($row['column_key'] != 'PRI') {
        $this->nonpkarray[] = $column_name;
        $this->nonpkcolumns .= $comma.$column_name;
        $comma = ',';
      }
    }

    // for PK columns we need them to be in correct order as well.
    $query = "select * from information_schema.statistics ".
             "where table_name = '%s' and TABLE_SCHEMA = '%s' ".
             "  and INDEX_NAME = 'PRIMARY' ".
             "order by INDEX_NAME, SEQ_IN_INDEX";

    $query = sprintf($query, $this->tablename, $this->dbname);

    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Failed to get PK column info '.$query);
    }

    $this->pkcolumnarray = array();
    while ($row =  mysql_fetch_assoc($result)) {
      $this->pkcolumnarray[] = quotify($row['COLUMN_NAME']);
    }

    if (!($this->flags & OSC_FLAGS_USE_NEW_PK) &&
          count($this->pkcolumnarray) === 0) {
      $this->raiseException("No primary key defined on the table!", false);
    }

  }

  // This is dependent on initColumnNameArray().
  // Uses the array of column names created by the former function
  // to come up with a string of comma separated columns.
  // It also builds strings of comma separated columns where each column is
  // prefixed with "NEW." and "OLD.".
  protected function initColumnNameStrings() {
    $this->columns = '';
    $this->oldcolumns = '';
    $this->newcolumns = '';
    $this->pkcolumns = '';
    $this->oldpkcolumns = '';
    $this->newpkcolumns = '';
    $comma = ''; // no comma at the front

    foreach ($this->columnarray as $column) {
      $this->columns .= $comma.$column;
      $this->oldcolumns .= $comma.'OLD.'.$column;
      $this->newcolumns .= $comma.'NEW.'.$column;
      $comma = ', '; // add comma from 2nd column
    }

    $comma = ''; // no comma at the front

    foreach ($this->pkcolumnarray as $column) {
      $this->pkcolumns .= $comma.$column;
      $this->oldpkcolumns .= $comma.'OLD.'.$column;
      $this->newpkcolumns .= $comma.'NEW.'.$column;
      $comma = ', '; // add comma from 2nd column
    }

  }

  protected function initRangeVariables() {
    $count = count($this->pkcolumnarray);
    $comma = ''; // no comma first time

    $this->rangeStartVars = '';
    $this->rangeStartVarsArray = array();
    $this->rangeEndVars = '';
    $this->rangeEndVarsArray = array();

    for ($i = 0; $i < $count; $i++) {
      $startvar = sprintf("@range_start_%d", $i);
      $endvar = sprintf("@range_end_%d", $i);
      $this->rangeStartVars .= $comma.$startvar;
      $this->rangeEndVars .= $comma.$endvar;
      $this->rangeStartVarsArray[] = $startvar;
      $this->rangeEndVarsArray[] = $endvar;

      $comma = ',';
    }
  }

  protected function refreshRangeStart() {
    $query = sprintf(" SELECT %s INTO %s ",
                     $this->rangeEndVars, $this->rangeStartVars);
    $this->executeSqlNoResult('Refreshing range start', $query);
  }

  protected function initLogFiles() {
    if (!empty($this->logfileFolder) && !file_exists($this->logfileFolder)) {
      $this->raiseException("Invalid logfile folder ".$this->logfileFolder,
                            false);
    }

    // where the osc log files go
    $this->oscLogFilePrefix = $this->logfileFolder.'__osclog_'.$this->tablename;
    $this->oscLogFP = fopen($this->oscLogFilePrefix.".log", 'a');
    $this->oscWarningFP = fopen($this->oscLogFilePrefix.".wrn", 'a');
    $this->oscErrorFP = fopen($this->oscLogFilePrefix.".err", 'a');
    fwrite($this->oscLogFP, $this->getOSCHeader());
  }

  // If non-null input_linkdir was passed to contructor, it gets validated
  // here. A new directory for database may also be created with proper
  // ownership.
  protected function checkInputSymlink() {
    if (empty($this->symlinkDir)) {
      return;
    }

    $dbdir = $this->getDataDir().$this->dbname.'/';

    if (($this->symlinkDir == $this->getDataDir()) ||
        ($this->symlinkDir == $dbdir)) {
      // we don't need to create a symlink in this case.
      // They just want to move the table to @@datadir.
      return;
    }

    if (!is_dir($this->symlinkDir)) {
      $this->raiseNonSqlException("Invalid symlink dir ".$this->symlinkDir);
    }

    // Create directory for the database under the symlink folder if it
    // does not already exist
    $newdir = $this->symlinkDir.$this->dbname;
    $this->symlinkDir = $newdir.'/';

    if (is_dir($newdir)) {
      return;
    }

    $cmd = "sudo -u mysql mkdir $newdir";
    $this->executeShellCmd("Mkdir", $cmd);

    if (!chmod($newdir, 0700)) {
      $this->raiseNonSqlException("chmod of $newdir to 0700 failed");
    }

  }

  // If null input_linkdir was passed to constructor, this checks if the
  // table is a symlink and if so, sets up $this->symlinkDir to point to
  // the dir in which table ibd file actually lives.
  protected function checkTableSymlink() {
    if (!empty($this->symlinkDir)) {
      return;
    }

    // we need to figure out if we need to create a symlink by checking if
    // table is currently a symlink
    $dbdir = $this->getDataDir().$this->dbname.'/';
    $tablefile = $dbdir.$this->tablename.".ibd";

    if (!file_exists($tablefile)) {
      // can't find the file
      // either IBD in wrong place, or table isn't a separate file
      $this->logWarning("Could not locate ibd file $tablefile");
      return;
    }

    if (!is_link($tablefile)) {
      return;
    }

    $link = readlink($tablefile);
    if (!$link) {
      $this->raiseNonSqlException("Readlink on $tablefile fails!");
    }

    $targetdir = dirname($link);

    if (!$targetdir) {
      $this->raiseNonSqlException("Could not get directory of link $link!");
    }

    // add / at the end
    $targetdir .= "/";
    $this->symlinkDir = $targetdir;
  }

  // Wrapper function to call
  // checkInputSymlink() if OSC caller supplied a input_linkdir (this means
  // caller wants to move the table to that dir)
  // OR
  // checkTableSymlink() if OSC caller did not supply input_linkdir (this
  // means just preserve table location i.e if newtable will live in same
  // dir as current table lives in)
  //
  // Symlink feature was implemented in 5.1.52 and so do nothing if it is
  // earlier version.
  protected function checkSymlink() {
    if ($this->version < "5.1.52") {
      return;
    }

    empty($this->symlinkDir) ?
      $this->checkTableSymlink() :
      $this->checkInputSymlink();

    $this->logCompact("symlinkDir has been set to $this->symlinkDir\n");
  }


  // Initializes names of files (names only and not contents) to be used as
  // OUTFILE targets in SELECT INTO
  protected function initOutfileNames() {
    $this->checkSymlink();

    // create outfiles in symlink dir if it exists
    if (empty($this->outfileFolder)) {
      $this->outfileFolder = $this->symlinkDir;
    }


    if (!empty($this->outfileFolder) && !file_exists($this->outfileFolder)) {
      $this->raiseException("Invalid outfile folder ".$this->outfileFolder,
                            false);
    }

    // if no folder specified for outfiles use @@secure_file_priv
    if (empty($this->outfileFolder)) {

      $query = 'select @@secure_file_priv as folder';

      if (!($result = mysql_query($query, $this->conn))) {
        $this->raiseException('Failed to get secure-file-priv system variable');
      }

      // we expect only one row
      while ($row =  mysql_fetch_assoc($result)) {
        $this->outfileFolder = $row['folder'];
      }

    }

    // if @@secure_file_priv is also empty, use @@datadir
    if (empty($this->outfileFolder)) {
      $this->outfileFolder = $this->getDataDir();

      // Add folder for this database
      $this->outfileFolder .= $this->dbname.'/';

    } else {
        // Make sure it ends with / but don't add two /
        $this->outfileFolder = rtrim($this->outfileFolder, '/').'/';
    }

    $this->outfileTable = $this->outfileFolder.'__osc_tbl_'.$this->tablename;
    $this->outfileExcludeIDs = $this->outfileFolder.
                                '__osc_ex_'.$this->tablename;
    $this->outfileIncludeIDs = $this->outfileFolder.
                                '__osc_in_'.$this->tablename;
  }

  // this should be called after validateVersion
  protected function decideIfReindex() {
    // If OSC does reindexing optimization and server crashes during OSC,
    // mysql bug http://bugs.mysql.com/bug.php?id=53256 causes server restart
    // to fail.
    // Facebook 5.1.52 has the fix and so do reindexing optimization unless
    // explicitly disabled. 
    return (($this->version >= "5.1.52") &&
            !($this->flags & OSC_FLAGS_NOREINDEX));
  }

  protected function validateVersion() {
    $query = 'select version() as version';
    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Failed to get version');
    }

    // we expect only one row
    while ($row =  mysql_fetch_assoc($result)) {
      $version = $row['version'];
    }

    $version_major = strtok($version, ".");
    $version_minor = strtok(".");
    $version_mini = strtok("_");

    $this->version = sprintf("%s.%s.%s", $version_major, $version_minor,
                                         $version_mini);

    if ((!($this->flags & OSC_FLAGS_ACCEPT_VERSION)) &&
        (($this->version < "5.0.84") || ($this->version > "5.1.63"))) {
      $error = "OSC has only been tested on versions 5.0.84, 5.1.47, 5.1.50 ".
               "and 5.1.52. Running on ".$this->version." is not allowed ".
               "unless OSC_FLAGS_ACCEPT_VERSION flag is set.";
      $this->raiseException($error, false);
    }
    return $this->version;
  }

  // checks for long running xact
  protected function checkLongXact() {
    if ($this->flags & OSC_FLAGS_LONG_XACT_OK) {
      return;
    }

    $query = "show full processlist";
    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Failed to get process list info '.$query);
    }

    $msg = '';
    $count = 0;
    while ($row =  mysql_fetch_assoc($result)) {
      if ((empty($row['Time']) || ($row['Time'] < $this->longXactTime)) ||
          ($row['db'] !== $this->dbname) || ($row['Command'] === 'Sleep')) {
        continue;
      }
      $count++;
      $buf = "Id=%d,user=%s,host=%s,db=%s,Command=%s,Time=%d,Info=%s\n";
      $msg .= sprintf($buf, $row['Id'], $row['User'], $row['Host'], $row['db'],
                     $row['Command'], $row['Time'], $row['Info']) ;
    }

    if ($count > 0) {
      $msg = sprintf("%d long running xact(s) found.\n".$msg, $count);
      $this->raiseException($msg, false);
    }

  }

  protected function init() {

    // store pkcolumns, all columns and nonpk columns
    $this->initColumnNameArrays();
    $this->initColumnNameStrings();

    $this->reindex = $this->decideIfReindex();

    $this->initRangeVariables();
    $this->initIndexes();

  }

  // creates a table (called deltas table) to capture changes to the table
  // being processed during the course of schema change.
  protected function createDeltasTable() {
    // deltas has an IDCOLNAME, DMLCOLNAME, and all columns as original table
    $createtable = 'create table %s'.
                   '(%s INT AUTO_INCREMENT, %s INT, primary key(%s)) '.
                   'ENGINE=InnoDB '.
                   'as (select %s from %s LIMIT 0)';
    $createtable = sprintf($createtable, $this->deltastable,
                           self::IDCOLNAME, self::DMLCOLNAME, self::IDCOLNAME,
                           $this->columns, $this->qtablenameq);

    $this->executeSqlNoResult('Creating deltas table', $createtable);
    $this->cleanupDeltastable = true;
  }

  // creates insert trigger to capture all inserts in deltas table
  protected function createInsertTrigger() {
    $trigger = 'create trigger %s AFTER INSERT ON %s FOR EACH ROW '.
               'insert into %s(%s, %s) '.
               'values (%d, %s)';
    $trigger = sprintf($trigger, $this->insertTrigger, $this->qtablenameq,
                       $this->deltastable, self::DMLCOLNAME, $this->columns,
                       self::DMLTYPE_INSERT, $this->newcolumns);
    $this->executeSqlNoResult('Creating insert trigger', $trigger);
    $this->cleanupInsertTrigger = true;
  }

  // Creates delete trigger to capture all deletes in deltas table
  // We must dump all columns or else we will encounter issues with
  // columns which are NOT NULL and lack a default
  protected function createDeleteTrigger() {
    $trigger = 'create trigger %s AFTER DELETE ON %s FOR EACH ROW '.
               'insert into %s(%s, %s) '.
               'values (%d, %s)';
    $trigger = sprintf($trigger, $this->deleteTrigger, $this->qtablenameq,
                       $this->deltastable, self::DMLCOLNAME, $this->columns,
                       self::DMLTYPE_DELETE, $this->oldcolumns);
    $this->executeSqlNoResult('Creating delete trigger', $trigger);
    $this->cleanupDeleteTrigger = true;
  }

  // creates update trigger to capture all updates in deltas table
  protected function createUpdateTrigger() {
    // if primary key is updated, map the update to delete followed by insert
    $trigger = 'create trigger %s AFTER UPDATE ON %s FOR EACH ROW  '.
               'IF (%s) THEN '.
               '  insert into %s(%s, %s) '.
               '  values(%d, %s); '.
               'ELSE '.
               '  insert into %s(%s, %s) '.
               '  values(%d, %s), '.
               '        (%d, %s); '.
               'END IF';
    $trigger = sprintf($trigger, $this->updateTrigger, $this->qtablenameq,
                       $this->getMatchCondition('NEW', 'OLD'),
                       $this->deltastable, self::DMLCOLNAME, $this->columns,
                       self::DMLTYPE_UPDATE, $this->newcolumns,
                       $this->deltastable, self::DMLCOLNAME, $this->columns,
                       self::DMLTYPE_DELETE, $this->oldcolumns,
                       self::DMLTYPE_INSERT, $this->newcolumns);

    $this->executeSqlNoResult('Creating update trigger', $trigger);
    $this->cleanupUpdateTrigger = true;
  }

  /**
   * The function exists because if lock table is run against a
   * table being backed up, then the table will be locked until
   * the end of the dump. If that happens then Online Schema Change
   * is not so online
   */
  protected function killSelects($table) {
    $sql = "SHOW FULL PROCESSLIST ";
    if (!($result = mysql_query($sql, $this->conn))) {
      $this->raiseException('Failed to get backup connections');
    }

    while ($row = mysql_fetch_array($result) ) {

      if( $row['db'] == $this->dbname &&
          $row['User'] == $this->backup_user &&
          stripos($row['Info'], 'SELECT ') !== FALSE &&
          stripos($row['Info'],'INFORMATION_SCHEMA') === FALSE){
        $kill = sprintf("KILL %s",$row[0]);
        // Note, we should not throw an exception if the kill fails.
        // The connection might have gone away on its own.
        $this->executeSqlNoResult("Killing dump query",$kill,
                                   self::LOGFLAG_WARNING);
      }
    }
  }


  /**
   * Important Assumption: Retrying on deadlock/timeout error assumes
   * that lock tables is the first step in a transaction. Otherwise
   * other locks acquired prior to lock tables could be released and it
   * won't make sense to just retry lock tables.
   */
  protected function lockTables($lock_reason, $lock_both_tables) {
    $this->killSelects($this->qtablenameq);

    if($lock_both_tables) {
      $this->killSelects($this->newtablename);
      $lock = sprintf('lock table %s WRITE, %s WRITE',
                $this->qtablenameq, $this->newtablename);
    } else {
      $lock =  sprintf('lock table %s WRITE', $this->qtablenameq);
    }

    $i = 0;
    $logflags = ((++$i < self::LOCK_MAX_ATTEMPTS) ? self::LOGFLAG_WARNING : 0);
    while (!$this->executeSqlNoResult($lock_reason, $lock, $logflags)) {
      $errno = mysql_errno($this->conn);
      $error = mysql_error($this->conn);
      // 1205 is timeout and 1213 is deadlock
      if (($errno == 1205) || ($errno == 1213) ||
          ($this->flags & OSC_FLAGS_ERRORTEST)) {
        $logflags = ((++$i < self::LOCK_MAX_ATTEMPTS) ?
                     self::LOGFLAG_WARNING : 0);
        continue;
      }
      // unknown error
      $this->raiseException($lock_reason.' failed. SQL:-'.$lock);
    }

  }

  protected function createTriggers() {
    $this->stopSlave();

    // without turning off autocommit lock tables is not working
    $this->executeSqlNoResult('AUTOCOMMIT OFF', 'set session autocommit=0');

    // In 5.0 version creating a trigger after locking a table causes hang.
    // So we will lock a little later.
    // Refer intern/wiki/index.php/Database/Online_Schema_Change_Testing and
    // search for workaround for more info.
    if ($this->version !== "5.0.84") {
      // false means lock only original table
      $this->lockTables('LOCKING table to drain pre-trigger Xacts', false);
    }

    $this->createInsertTrigger();

    $this->createDeleteTrigger();

    $this->createUpdateTrigger();

    // for other version we have already locked above.
    if ($this->version === "5.0.84") {
      // false means lock only original table
      $this->lockTables('LOCKING table to drain pre-trigger Xacts', false);
    }


    $this->executeSqlNoResult('COMMITTING', 'COMMIT');

    $this->executeSqlNoResult('Unlocking after triggers', 'unlock tables');

    $this->executeSqlNoResult('AUTOCOMMIT ON', 'set session autocommit=1');

    $this->startSlave();

  }


  // Used for creating temp tables for IDs to exclude or IDs to include
  protected function createAndInitTemptable($temptable) {
    if ($temptable === self::TEMP_TABLE_IDS_TO_EXCLUDE) {
      $outfile = $this->outfileExcludeIDs;

      $selectinto = "select %s, %s ".
                    "from %s ".
                    "order by %s into outfile '%s' ";
      $selectinto = sprintf($selectinto,
                            self::IDCOLNAME, self::DMLCOLNAME,
                            $this->deltastable,
                            self::IDCOLNAME, $outfile);

    } else if ($temptable === self::TEMP_TABLE_IDS_TO_INCLUDE) {
      $outfile = $this->outfileIncludeIDs;

      // Select from deltastable that are not in TEMP_TABLE_IDS_TO_EXCLUDE.
      // Use left outer join rather than 'in' subquery for better perf.
      $idcol = $this->deltastable.'.'.self::IDCOLNAME;
      $dmlcol = $this->deltastable.'.'.self::DMLCOLNAME;
      $idcol2 = self::TEMP_TABLE_IDS_TO_EXCLUDE.'.'.self::IDCOLNAME;
      $selectinto = "select %s, %s ".
                    "from %s LEFT JOIN %s ON %s = %s ".
                    "where %s is null order by %s into outfile '%s' ";
      $selectinto = sprintf($selectinto, $idcol, $dmlcol,
                            $this->deltastable, self::TEMP_TABLE_IDS_TO_EXCLUDE,
                            $idcol, $idcol2, $idcol2, $idcol, $outfile);
    } else {
      $this->raiseException("Invalid param temptable : $temptable", false);
    }

    $this->executeSqlNoResult('Selecting ids from deltas to outfile',
                              $selectinto);

    $this->cleanupOutfile = $outfile;


    $createtemp = 'create temporary table %s(%s INT,
                                             %s INT,
                                             primary key(%s)) engine=myisam';
    $createtemp = sprintf($createtemp, $temptable,
                          self::IDCOLNAME, self::DMLCOLNAME, self::IDCOLNAME);
    $this->executeSqlNoResult('Creating temp table for ids to exclude',
                              $createtemp);

    // read from outfile above into the temp table
    $loadsql = sprintf("LOAD DATA INFILE '%s' INTO TABLE %s(%s, %s)",
                       $outfile, $temptable, self::IDCOLNAME, self::DMLCOLNAME);
    $this->executeSqlNoResult('Loading ids to exclude ', $loadsql);

    unset($this->cleanupOutfile);
    $this->executeUnlink($outfile);
  }

  protected function startSnapshotXact() {

    $this->executeSqlNoResult('starting transaction',
                              'START TRANSACTION WITH CONSISTENT SNAPSHOT');
    // any deltas captured so far need to be excluded because they would
    // already be reflected in our snapshot.
    $this->createAndInitTemptable(self::TEMP_TABLE_IDS_TO_EXCLUDE);

  }

  // Generates assignment condition of the form
  // @var1 := col1, @var2 := col2, ....
  protected function assignRangeEndVariables($columns, $variables) {
    if (!$columns) {
      return '';
    }
    $count = count($columns);
    $comma = ''; // no comma first time
    $assign = '';
    for ($i = 0; $i < $count; $i++) {
      $assign .= $comma.sprintf('%s := %s', $variables[$i], $columns[$i]);
      $comma = ',';
    }
    return $assign;
  }

  /**
    Given a list of columns and a list of values (of same length), produce a
    'greater than' SQL condition by splitting into multiple conditions.
    An example result may look like:
    ((col1 > val1) OR
     ((col1 = val1) AND (col2 > val2)) OR
     ((col1 = val1) AND (col2 = val2) AND (col3 > val3)))
    Which stands for (col1, col2, col3) > (val1, val2, val3).
    The latter being simple in representation, however MySQL does not utilize
    keys properly with this form of condition, hence the splitting into multiple
    conditions.
    It can also be used for >=, < or <= but we don't need them now.
  */
  protected function getRangeStartCondition($columns, $values,
                                          $comparison_sign = ">")  {
    $comparison = '';
    $count = count($columns);
    $equality = '';
    $range = '';
    $and = ''; // no AND first time
    $or = '';
    for ($i = 0; $i < $count; $i++) {
      // compare condition for this column
      $range = sprintf(" %s %s %s ", $columns[$i], $comparison_sign,
                       $values[$i]);

      // equality comparison for all previous columns
      if ($i > 0) {
        $equality .= $and.sprintf(" %s = %s ", $columns[$i-1], $values[$i-1]);
        $and = ' AND ';
      }

      // Note that when $i is 0, both $equality and $and will be empty
      $comparison .= $or.'('.$equality.$and.$range.')';
      $or = ' OR ';
    }
    // enclose in ()
    return sprintf('(%s)', $comparison);
  }

  protected function selectFullTableIntoOutfile() {

    $selectinto = "select %s ".
                  "FROM %s ".
                  "INTO OUTFILE '%s.1'";

    $selectinto = sprintf($selectinto, $this->columns,
                          $this->qtablenameq, $this->outfileTable);


    $this->executeSqlNoResult('Selecting full table into outfile',
                              $selectinto);

    $this->outfileSuffixStart = 1;
    $this->outfileSuffixEnd = 1;

    $this->executeSqlNoResult('Committing after generating outfiles', 'COMMIT');
  }

  protected function selectTableIntoOutfile() {
    // we want to do the full table dump/load since we can't page
    if ($this->flags & OSC_FLAGS_USE_NEW_PK) {
      return $this->selectFullTableIntoOutfile();
    }

    $whereclause = ''; // first time there is no where clause

    $outfile_suffix = 0;

    do {
      $outfile_suffix++; // start with 1

      $selectinto = "select %s, %s ".
                    "FROM %s FORCE INDEX (PRIMARY) %s ".
                    "ORDER BY %s LIMIT %d ".
                    "INTO OUTFILE '%s.%d'";

      // this gets pk column values into range end variables
      $assign = $this->assignRangeEndVariables($this->pkcolumnarray,
                                               $this->rangeEndVarsArray);
      $selectinto = sprintf($selectinto, $assign, $this->nonpkcolumns,
                            $this->qtablenameq, $whereclause,
                            $this->pkcolumns, $this->batchsizeLoad,
                            $this->outfileTable, $outfile_suffix);
      $this->executeSqlNoResult('Selecting table into outfile', $selectinto);
      $this->outfileSuffixStart = 1;
      $this->outfileSuffixEnd = $outfile_suffix;
      $rowCount =  mysql_affected_rows($this->conn);

      $this->refreshRangeStart();
      $range = $this->getRangeStartCondition($this->pkcolumnarray,
                                             $this->rangeStartVarsArray);
      $whereclause = sprintf(" WHERE %s ", $range);
    } while ($rowCount >= $this->batchsizeLoad);

    $this->executeSqlNoResult('Committing after generating outfiles', 'COMMIT');
  }

  // gets @@datadir into $this->dataDir and returns it as well
  // ensures that datadir ends with /
  protected function getDataDir() {
    if (!empty($this->dataDir)) {
      return $this->dataDir;
    }

    $query = 'select @@datadir as dir';

    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Failed to get datadir system variable');
    }

    // we expect only one row
    while ($row =  mysql_fetch_assoc($result)) {
      // add / at the end but only if it does not already have one
      $this->dataDir = $row['dir'];
      if (empty($this->dataDir)) {
        $this->raiseException("Datadir is empty", false);
      } else {
        // Make sure it ends with / but don't add two /
        $this->dataDir = rtrim($this->dataDir, '/').'/';
      }

      return $this->dataDir;
    }

    $this->raiseException("Query to get datadir returned no rows");
  }

  // If $this->symlinkDir is set and it is different from datadir, then the
  // new table needs to be a symlink with the actual table file living in
  // the symlinkDir.
  //
  // In rare error cases, such as if OSC fails after creating symlink
  // but before __osc_new_T is created, the link is not cleaned up.
  // That is fine because it does not consume space and next run of
  // OSC can handle such links left over from previous runs. (If OSC
  // fails after __osc_new_T is created, then it will drop __osc_new_T
  // and the link will get cleaned up at that point.)
  protected function createSymlinkIfNeeded() {
    if ($this->version < "5.1.52") {
      return;
    }

    $dbdir = $this->getDataDir().$this->dbname.'/';
    if (empty($this->symlinkDir) ||
        ($this->symlinkDir == $dbdir) ||
        ($this->symlinkDir == $this->getDataDir())) {
      return;
    }

    $newtablelink = $dbdir.$this->newtablename.".ibd";
    if (is_link($newtablelink)) {
      // Link already seems to exist. Perhaps it did not get cleaned up
      // from an earlier run
      $this->logWarning("Link $newtablelink already exists!\n");
    }

    $newtablefile = $this->symlinkDir.$this->newtablename.".ibd";
    $cmd = "sudo -u mysql ln -s $newtablefile $newtablelink";
    $this->executeShellCmd("Create Symlink", $cmd);
  }

  protected function createCopyTable() {
    $this->createSymlinkIfNeeded();
    $this->executeSqlNoResult('Creating copy table', $this->createcmd);
    $this->cleanupNewtable = true;

    if (! (bool)($this->flags & OSC_FLAGS_DROPCOLUMN) ) {
      $query = " SELECT c1.COLUMN_NAME ".
               " FROM information_schema.columns c1 ".
               " LEFT join information_schema.columns c2 ON ".
               "   c1.COLUMN_NAME =  c2.COLUMN_NAME AND ".
               "   c1.TABLE_SCHEMA = c2.TABLE_SCHEMA  AND ".
               "   c2.table_name = '%s' ".
               " WHERE c1.table_name = '%s' AND ".
               "   c1.TABLE_SCHEMA = '%s' AND ".
               "   c2.COLUMN_NAME IS NULL";
      $query = sprintf($query,
               $this->newtablename, $this->tablename, $this->dbname);
      $result = mysql_query($query, $this->conn);

      while ($row = mysql_fetch_array($result) ) {
        $this->raiseException(
          'A column in the existing table is not in newtable.');
      }
    }
  }

  // replace 'create table <tablename> ...' with 'create table <copy table> ...
  protected function modifyDDL() {
    $count = 0;
    // replace alter table with upper case if it was in lower case
    $createcopy = preg_replace('/create\s+table\s+/i',
                              'CREATE TABLE ',
                              $this->createcmd, -1, $count);
    if ($count != 1) {
      $error = "Found ".$count." matches for 'create table' in ".
               "the DDL command.\nExpecting exactly 1 match. ".
               "Please check DDL:\n".$this->createcmd."\n";
      $this->raiseException($error, false);
    }

    // if the tablename is a reserved word they may have enclosed it in
    // backquotes and so try looking for tablename as well as backquoted
    // tablename
    $count1 = 0;
    $createcopy1 = preg_replace('/CREATE\s+TABLE\s+'.$this->tablename.'/',
                              'CREATE TABLE '.$this->newtablename,
                              $createcopy, -1, $count1);


    $count2 = 0;
    $createcopy2 = preg_replace('/CREATE\s+TABLE\s+'.$this->qtablenameq.'/',
                              'CREATE TABLE '.$this->newtablename,
                              $createcopy, -1, $count2);

    $count = $count1 + $count2;
    if ($count != 1) {
      $error = "Found ".$count." matches for 'CREATE TABLE ".$this->tablename.
               "' in the DDL command.\nExpecting exactly 1 match. ".
               "Please check DDL:\n".$createcopy."\n";
      $this->raiseException($error, false);
    } else if ($count1 == 1) {
      $this->createcmd = $createcopy1;
    } else {
      $this->createcmd = $createcopy2;
    }
  }

  // validates any assumptions about PK after the alter
  protected function validatePostAlterPK($primary) {
    if (empty($primary)) {
      $this->raiseException("No primary key defined in the new table!", false);
    }

    if ($this->flags & OSC_FLAGS_ACCEPTPK) {
      return;
    }

    if ($this->flags & OSC_FLAGS_USE_NEW_PK) {
      // for this mode we need to ensure that all columns in the new PK
      // are already part of the old version of the table
      foreach ($this->pkcolumnarray as $col) {
        if (!in_array($col, $this->columnarray)) {
          $error = "You can not create a new PK using new columns.  ".
                   "The columns must already exist in the old table.";
          $this->raiseException($error, false);
        }
      }
    }

    // check if old PK (available in $this->pkcolumnarry) is a prefix
    // of atleast one index after the alter table.
    // Note that if old PK is (a, b) and after alter table there is an
    // index on (b, a), that is OK as it supports efficient lookups
    // if values of both a and b are provided.
    $pkcount = count($this->pkcolumnarray);
    foreach ($this->indexes as $index) {
      // get an array of index column names
      $colarray = array();
      foreach ($index->columns as $column) {
        $colarray[] = $column->name;
      }

      // get an array slice of 1st pkcount elements
      $prefix = array_slice($colarray, 0, $pkcount);

      $diff = array_diff($this->pkcolumnarray, $prefix);

      // if A and B are equal size and there are no elements in A
      // that are not in B, it means A and B are same.
      if ((count($prefix) === $pkcount) && empty($diff)) {
        return;
      }
    }

    $error = "After alter there is no index on old PK columns. May not ".
             "support efficient lookups using old PK columns. ".
             "Not allowed unless OSC_FLAGS_ACCEPTPK is set.";
    $this->raiseException($error, false);
  }

  // Retrieves info about indexes on copytable
  protected function initIndexes() {
    $query = "select * from information_schema.statistics ".
             "where table_name = '%s' and TABLE_SCHEMA = '%s' ".
             "order by INDEX_NAME, SEQ_IN_INDEX";

    $query = sprintf($query, $this->newtablename, $this->dbname);

    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Failed to get index info '.$query);
    }

    // save index info as array
    $this->indexes = array();

    // we are resetting the PK so that it will be used in later steps
    if ($this->flags & OSC_FLAGS_USE_NEW_PK) {
      $this->pkcolumnarray = array();
    }
    $prev_index_name = '';
    $index = null;
    $primary = null;
    while ($row =  mysql_fetch_assoc($result)) {
      $index_name = quotify($row['INDEX_NAME']);
      $column_name = quotify($row['COLUMN_NAME']);
      if ($prev_index_name != $index_name) {
        // is the 1st column of the index autoincrement column?
        $auto = isset($this->autoIncrement) &&
                ($column_name === $this->autoIncrement);
        $index = new IndexInfo($this->newtablename, $index_name,
                               $row['NON_UNIQUE'], $auto);
        if ($index->isPrimary()) {
          $primary = $index;
        }
        $this->indexes[] = $index;
      }
      $index->addColumn($column_name, $row['SUB_PART']);

      if ($this->flags & OSC_FLAGS_USE_NEW_PK && $index->isPrimary()) {
        $this->pkcolumnarray[] = $column_name;
      }
      $prev_index_name = $index_name;
    }

    // re-create these strings with new array
    if ($this->flags & OSC_FLAGS_USE_NEW_PK) {
      $this->initColumnNameStrings();
    }

    $this->validatePostAlterPK($primary);
    $this->joinClauseReplay = $this->getJoinClauseReplay();
  }

  protected function dropNCIndexes() {
    if (!$this->reindex) {
      return;
    }

    foreach ($this->indexes as $index) {
      if (( !$index->isPrimary() && !$index->isAutoIncrement )
          && !($this->flags & OSC_FLAGS_ELIMINATE_DUPS && !$index->nonUnique)) {
        $drop = $index->getDropSql();
        $this->executeSqlNoResult('Dropping index', $drop);
      }
    }

  }

  protected function recreateNCIndexes() {
    if (!$this->reindex) {
      return;
    }
    $count = 0;
    $create = 'Alter table '.$this->newtablename;
    $comma = ''; // no comma first time
    foreach ($this->indexes as $index) {
      if (!$index->isPrimary() && !$index->isAutoIncrement
          && !($this->flags & OSC_FLAGS_ELIMINATE_DUPS && !$index->nonUnique)) {
        $create .= $comma.$index->getCreateSql();
        $comma = ',';
        $count++;
      }
    }
    if ($count > 0) {
      $this->executeSqlNoResult('ReCreating NC indexes', $create);
    }
  }

  // Every now and again index stats are wonky after an OSC run
  protected function analyzeTableIndexs() {
    $analyze = "ANALYZE TABLE ".$this->newtablename;
    $this->executeSqlNoResult('ANALYZE TABLE', $analyze);
  }

  // loads copy table from outfile
  protected function loadCopyTable() {
    while ($this->outfileSuffixEnd >= $this->outfileSuffixStart) {
      if ($this->flags & OSC_FLAGS_USE_NEW_PK) {
        $loadsql = sprintf("LOAD DATA INFILE '%s.%d' %s INTO TABLE %s(%s)",
                           $this->outfileTable,
                           $this->outfileSuffixStart,
                           $this->ignoredups,
                           $this->newtablename,
                           $this->columns);
      }
      else {
        $loadsql = sprintf("LOAD DATA INFILE '%s.%d' %s INTO TABLE %s(%s, %s)",
                           $this->outfileTable,
                           $this->outfileSuffixStart,
                           $this->ignoredups,
                           $this->newtablename,
                           $this->pkcolumns, $this->nonpkcolumns);
      }
      // the LOAD might fail if duplicate keys were added in a new PK
      if (!$this->executeSqlNoResult('Loading copy table',
                                    $loadsql,
                                    self::LOGFLAG_WARNING)) {
        if (mysql_errno($this->conn) == 1062) {
          $this->raiseException("Duplicate key found while loading table. ".
                                "Most likely a problem with new PK: ");
        }
        $this->raiseException("Error loading data: ");
      }

      // delete file now rather than waiting till cleanup
      // as this will free up space.
      $filename = sprintf('%s.%d', $this->outfileTable,
                          $this->outfileSuffixStart);
      $this->outfileSuffixStart++;
      if (!($this->flags & OSC_FLAGS_NOCLEANUP)) {
        $this->executeUnlink($filename);
      }
    }
    unset($this->outfileSuffixEnd);
    unset($this->outfileSuffixStart);
  }

  // Generates condition of the form
  // tableA.column1=tableB.column1 AND tableA.column2=tableB.column2 ...
  // If null $columns is passed, it uses $this->pkcolumnarray as array.
  protected function getMatchCondition($tableA, $tableB, $columns = null) {
    if ($columns === null) {
      $columns = $this->pkcolumnarray;
    }

    $cond = '';
    $and = ''; // no AND first time
    foreach ($columns as $column) {
      $cond .= $and.sprintf(' %s.%s = %s.%s ',
                            $tableA, $column, $tableB, $column);
      $and = ' AND ';
    }

    $cond .= ' ';

    return $cond;
  }

  // Builds the join clause used during replay.
  // Join condition of the form A.col1 = B.col1 AND A.col2=B.col2 AND ...
  // where A is copytable, B is deltastable AND col1, col2, ... are the
  // PK columns before ALTER.
  protected function getJoinClauseReplay() {
    return ($this->getMatchCondition($this->newtablename, $this->deltastable));
  }

  // check that replay command has affected exactly one row
  protected function validateReplay($replay_sql) {
    $count = mysql_affected_rows($this->conn);
    if ($count > 1 ||
      ($count == 0 && !($this->flags & OSC_FLAGS_ELIMINATE_DUPS))) {
      $error = sprintf('Replay command [%s] affected %d rows instead of 1 row',
                       $replay_sql, $count);
      $this->raiseException($error, false);
    }
  }

  // Row has ID that can be used to look up into deltas table
  // to find PK of the row in the newtable to delete
  protected function replayDeleteRow($row) {
    $newtable = $this->newtablename;
    $deltas = $this->deltastable;
    $delete = sprintf('delete %s from %s, %s where %s.%s = %d AND %s',
                      $newtable, $newtable, $deltas, $deltas, self::IDCOLNAME,
                      $row[self::IDCOLNAME],
                      $this->joinClauseReplay);
    $this->executeSqlNoResult('Replaying delete row', $delete,
                              self::LOGFLAG_VERBOSE);
    $this->validateReplay($delete);
  }

  // Row has ID that can be used to look up into deltas table
  // to find PK of the row in the newtable to update.
  // New values for update (only non-PK columns are updated) are
  // all taken from deltas table.
  protected function replayUpdateRow($row) {
    $assignment = '';
    $comma = ''; // no comma first time
    foreach ($this->nonpkarray as $column) {
      $assignment .= $comma.$this->newtablename.'.'.$column.'='.
                            $this->deltastable.'.'.$column;
      $comma = ', ';
    }

    $newtable = $this->newtablename;
    $deltas = $this->deltastable;
    $update = sprintf('update %s %s, %s SET %s where %s.%s = %d AND %s ',
                      $this->ignoredups, $newtable, $deltas, $assignment,
                      $deltas, self::IDCOLNAME,
                      $row[self::IDCOLNAME],
                      $this->joinClauseReplay);
    $this->executeSqlNoResult('Replaying update row', $update,
                              self::LOGFLAG_VERBOSE);
    // if original update had old value same as new value, trigger fires
    // and row gets inserted into deltas table. However mysql_affected_rows
    // would return 0 for replay update. So this validation is commented out
    // for now.
    // $this->validateReplay($update);
  }

  // Row has ID that can be used to look up into deltas table
  // to find the row that needs to be inserted into the newtable.
  protected function replayInsertRow($row) {
    $insert = sprintf('insert %s into %s(%s) '.
                      'select %s from %s where %s.%s = %d',
                      $this->ignoredups, $this->newtablename, $this->columns,
                      $this->columns, $this->deltastable, $this->deltastable,
                      self::IDCOLNAME, $row[self::IDCOLNAME]);
    $this->executeSqlNoResult('Replaying insert row', $insert,
                              self::LOGFLAG_VERBOSE);
    $this->validateReplay($insert);
  }

  // Copies rows from self::TEMP_TABLE_IDS_TO_INCLUDE to
  // self::TEMP_TABLE_IDS_TO_EXCLUDE
  protected function appendToExcludedIDs() {
    $append = sprintf('insert into %s(%s, %s) select %s, %s from %s',
                      self::TEMP_TABLE_IDS_TO_EXCLUDE,
                      self::IDCOLNAME, self::DMLCOLNAME,
                      self::IDCOLNAME, self::DMLCOLNAME,
                      self::TEMP_TABLE_IDS_TO_INCLUDE);
    $this->executeSqlNoResult('Appending to excluded_ids', $append);
  }

  protected function replayChanges($single_xact) {
    // create temp table for included ids
    $this->createAndInitTemptable(self::TEMP_TABLE_IDS_TO_INCLUDE);

    $query = sprintf('select %s, %s from %s order by %s',
                     self::IDCOLNAME, self::DMLCOLNAME,
                     self::TEMP_TABLE_IDS_TO_INCLUDE, self::IDCOLNAME);
    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Query to replay changes failed.'.$query);
    }

    $i = 0; // iteration count
    $inserts = 0;
    $deletes = 0;
    $updates = 0;

    if (!$single_xact) {
      $this->executeSqlNoResult('Starting batch xact for replay',
                                'START TRANSACTION', self::LOGFLAG_VERBOSE);
    }

    while ($row = mysql_fetch_assoc($result)) {
      ++$i;
      if (!$single_xact && ($i % $this->batchsizeReplay == 0)) {
        $this->executeSqlNoResult('Commiting batch xact for replay',
                                  'COMMIT', self::LOGFLAG_VERBOSE);
      }

      switch ($row[self::DMLCOLNAME]) {
        case self::DMLTYPE_DELETE :
                              $this->replayDeleteRow($row);
                              $deletes++;
                              break;

        case self::DMLTYPE_UPDATE :
                              $this->replayUpdateRow($row);
                              $updates++;
                              break;

        case self::DMLTYPE_INSERT :
                              $this->replayInsertRow($row);
                              $inserts++;
                              break;

        default :
                              $this->raiseException('Invalid DML type', false);
      }
    }
    if (!$single_xact) {
      $this->executeSqlNoResult('Commiting batch xact for replay', 'COMMIT',
                                self::LOGFLAG_VERBOSE);
    }

    $this->appendToExcludedIDs();

    $drop = 'DROP TEMPORARY TABLE '.self::TEMP_TABLE_IDS_TO_INCLUDE;
    $this->executeSqlNoResult('Dropping temp table of included ids', $drop);

    $output = sprintf("Replayed %d inserts, %d deletes, %d updates\n",
                      $inserts, $deletes, $updates);
    $this->logCompact($output);
  }

  protected function checksum() {
    $query = sprintf("checksum table %s, %s",
                     $this->newtablename, $this->qtablenameq);

    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Failed to get checksums: '.$query);
    }

    // we expect only two rows
    $i = 0;
    $checksum = array();
    while ($row =  mysql_fetch_assoc($result)) {
      $checksum[$i++] = $row['Checksum'];
    }

    if ($checksum[0] != $checksum[1]) {
      $error = "Checksums don't match.".$checksum[0]."/".$checksum[1];
      $this->raiseException($error, false);
    }

  }

  protected function swapTables() {
    $this->stopSlave();

    // without turning off autocommit lock tables is not working
    $this->executeSqlNoResult('AUTOCOMMIT OFF', 'set session autocommit=0');

    // true means lock both original table and newtable
    $this->lockTables('Locking tables for final replay/swap', true);

    // any changes that happened after we replayed changes last time.
    // true means do them in one transaction.
    $this->replayChanges(true);

    // at this point tables should be identical if schema is same
    if ($this->flags & OSC_FLAGS_CHECKSUM) {
      $this->checksum();
    }

    $rename_original = sprintf('alter table %s rename %s',
                               $this->qtablenameq, $this->renametable);
    $this->executeSqlNoResult('Renaming original table', $rename_original);
    $this->cleanupRenametable = true;

    // if the above command succeeds and the following command fails,
    // we will have:
    // $this->cleanupNewtable set and $this->cleanupRenametable set.
    // In that case we will rename renametable back to original tablename.
    $rename_new = sprintf('alter table %s rename %s',
                          $this->newtablename, $this->qtablenameq);
    $this->executeSqlNoResult('Renaming new table', $rename_new);
    unset($this->cleanupNewtable);

    $this->executeSqlNoResult('COMMITTING', 'COMMIT');

    $this->executeSqlNoResult('Unlocking tables', 'unlock tables');

    $this->executeSqlNoResult('AUTOCOMMIT ON', 'set session autocommit=1');

    $this->startSlave();

  }

  protected function doesTableExist($tablename) {
    $query = sprintf("show tables like '%s'", $tablename);

    if (!($result = mysql_query($query, $this->conn))) {
      $this->raiseException('Failed to get table existence info:'.$query);
    }

    return (mysql_fetch_assoc($result) ? true : false);
  }

  protected function cleanup() {
    if ($this->flags & OSC_FLAGS_NOCLEANUP) {
      return;
    }

    // if we don't have a connection get it
    if (!$this->conn || !mysql_ping($this->conn)) {
      $this->openAndInitConnection();
    }

    $force = $this->flags & OSC_FLAGS_FORCE_CLEANUP;

    $this->executeSqlNoResult('Unlock tables just in case', 'unlock tables');

    $this->executeSqlNoResult('Rollback in case we are in xact', 'ROLLBACK');

    // in case we are in autocommit off, turn it on
    $this->executeSqlNoResult('AUTOCOMMIT ON', 'set session autocommit=1');

    if ($force) {
      $this->cleanupInsertTrigger = true;
      $this->cleanupDeleteTrigger = true;
      $this->cleanupUpdateTrigger = true;
    }
    if (isset($this->cleanupInsertTrigger)) {
      $drop = sprintf('drop trigger %s.%s',
                      $this->qdbnameq, $this->insertTrigger);
      $this->executeSqlNoResult('Dropping insert trigger', $drop);
      unset($this->cleanupInsertTrigger);
    }

    if (isset($this->cleanupDeleteTrigger)) {
      $drop = sprintf('drop trigger %s.%s',
                      $this->qdbnameq, $this->deleteTrigger);
      $this->executeSqlNoResult('Dropping delete trigger', $drop);
      unset($this->cleanupDeleteTrigger);
    }

    if (isset($this->cleanupUpdateTrigger)) {
      $drop = sprintf('drop trigger %s.%s',
                      $this->qdbnameq, $this->updateTrigger);
      $this->executeSqlNoResult('Dropping update trigger', $drop);
      unset($this->cleanupUpdateTrigger);
    }

    if ($force) {
      $this->cleanupDeltastable = true;
      $this->cleanupNewtable = true;

      // We need to be careful when dropping renamedtable because
      // during previous run, we may have failed AFTER original
      // table was renamed. If we drop renamed table, we may lose
      // the table.
      if ($this->doesTableExist($this->renametable)) {
        $this->cleanupRenametable = true;
      }
    }

    if (isset($this->cleanupDeltastable)) {
      $this->executeSqlNoResult('Dropping deltas table',
                                'drop table '.$this->deltastable);
      unset($this->cleanupDeltastable);
    }

    // does original table exist
    $orig_table_exists = $this->doesTableExist($this->tablename);

    if (isset($this->cleanupRenametable) && !$orig_table_exists) {
      // rename renametable back to original name.
      $warning = "Original table does not exist but renamed table exists!. ".
                 "Must have failed AFTER renaming original table!";
      $this->logWarning($warning);

      $rename = sprintf('alter table %s rename %s',
                        $this->renametable, $this->qtablenameq);
      $this->executeSqlNoResult('Renaming backup table as original table',
                                $rename);
      unset($this->cleanupRenametable);
    } else if (!$orig_table_exists) {
      // PANIC
      $this->raiseException("NEITHER ORIGINAL TABLE EXISTS NOR RENAMED TABLE",
                            false);
    } else if (isset($this->cleanupRenametable)) {
      if ($this->flags & OSC_FLAGS_DROPTABLE) {
        $this->dropTable($this->renametable,$this->conn);
        unset($this->cleanupRenametable);
      }
    }

    if (isset($this->cleanupNewtable)) {
      $this->dropTable($this->newtablename,$this->conn);
      unset($this->cleanupNewtable);
    }

    // in case we stopped slave, start it
    $this->startSlave();

    if (isset($this->cleanupOutfile)) {
      $outfile = $this->cleanupOutfile;
      $this->executeUnlink($outfile);
      unset($this->cleanupOutfile);
    } else if ($force) {
      if (isset($this->outfileIncludeIDs)) {
        $this->executeUnlink($this->outfileIncludeIDs);
      }
      if (isset($this->outfileExcludeIDs)) {
        $this->executeUnlink($this->outfileExcludeIDs);
      }
    }

    if (isset($this->outfileSuffixEnd) && isset($this->outfileSuffixStart)) {
      while ($this->outfileSuffixEnd >= $this->outfileSuffixStart) {
        $filename = sprintf('%s.%d', $this->outfileTable,
                            $this->outfileSuffixStart);
        $this->executeUnlink($filename);
        $this->outfileSuffixStart++;
      }
      unset($this->outfileSuffixEnd);
      unset($this->outfileSuffixStart);
    } else if ($force && isset($this->outfileTable) ) {
      $files_wildcard = sprintf('%s.*', $this->outfileTable);
      $files = glob($files_wildcard);
      foreach ($files as $file) {
        $this->executeUnlink($file);
      }
    }

    if ($this->flags & OSC_FLAGS_DELETELOG) {
      // true means check if file exists
      $this->executeUnlink($this->oscLogFilePrefix.".log", true);
      $this->executeUnlink($this->oscLogFilePrefix.".wrn", true);
      $this->executeUnlink($this->oscLogFilePrefix.".err", true);
    }

    // closing connection should drop temp tables
    // don't bother checking return status as this is last step anyway.
    if ($this->conn) {
      $this->releaseOscLock($this->conn); // noop if lock not held
      mysql_close($this->conn);
      $this->conn = null;
    }

  }

  public function forceCleanup() {
    $this->flags |= OSC_FLAGS_FORCE_CLEANUP;
    $this->flags &= ~OSC_FLAGS_ERRORTEST;
    return $this->execute();
  }

  public function execute() {
    if (!$this->conn || !mysql_ping($this->conn)) {
      $this->openAndInitConnection();
    }

    try {
      $this->validateVersion();
      // outfile names for storing copy of table, and processed IDs
      $this->initOutfileNames();

      if ($this->flags & OSC_FLAGS_FORCE_CLEANUP) {
        $this->cleanup();
        return true;
      } else {
        $this->modifyDDL();
        if ($this->doesTableExist($this->renametable)) {
          $error = sprintf("Please cleanup table %s left over from prior run.",
                           $this->renametable);
          $this->raiseException($error, false);
        }

        $this->checkLongXact();
      }

      $this->createCopyTable();
      // we call init() after the create/alter since we need the new columns
      $this->init();
      $this->createDeltasTable();
      $this->createTriggers();
      $this->startSnapshotXact();
      $this->selectTableIntoOutfile();
      $this->dropNCIndexes();
      $this->loadCopyTable();
      $this->replayChanges(false); // false means not in single xact
      $this->recreateNCIndexes();
      $this->analyzeTableIndexs();
      $this->replayChanges(false); // false means not in single xact
      $this->swapTables();
      $this->cleanup();
      return true;
    } catch (Exception $e) {
      // it is possible that we got exception during cleanup().
      // that is fine, we will try once more to cleanup remaining
      // resources.
      $this->logError('Caught exception: '. $e->getMessage(). "\n");
      $this->cleanup();
    }
    return false;
  }
}


