<?php
/*
    Copyright 2011-Present Facebook

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

*/

$basedir = dirname(__FILE__);
$this_script = basename(__FILE__);
require_once $basedir.'/OnlineSchemaChange.php';
require_once $basedir.'/dba_lib_foss.php';

define('CONNECTION_LIMIT','500');
define('OSC_LOG_DIR','/var/tmp/');
define('OSC_LOG_PREFIX','__osclog_');
define('OLD_TABLE_PREFIX','__osc_old_');

ini_set('mysql.connect_timeout', 20);
ini_set('default_socket_timeout',1800);
ini_set('date.timezone','America/Los_Angeles');

if (function_exists('mysql_set_timeout')) {
  mysql_set_timeout(99999999);
}

$db_admin_user = 'root'; 
$db_admin_pass = '';
$backup_user = 'backup';

//////////////////////////////
// Main
//////////////////////////////

$validate_arg_array = array(
  "mode" => array(
    "required" => 0 ,
    "value_expected" => 1 ,
    "default_val" => 'create' ,
    "help_alias" => "[ seed | statement | clean ]"
    ),
  "ddl_file" => array(
    "required" => 0 ,
    "value_expected" => 1 ,
    "help_alias" => "Required for mode statement.
                     File with ALTER or CREATE statements"
    ),
  "seed_tables" => array(
    "required" => 0 ,
    "value_expected" => 1 ,
    "help_alias" => "Required for mode seed.
                     Comma seperated list of tables for which schema
                     should be replicated from seed db"
    ),
  "seed_host" => array(
    "required" => 0 ,
    "value_expected" => 1 ,
    "default_val" => null ,
    "help_alias" => "Required for mode seed.
                     Host with example of desired table structure"
    ),
  "seed_db" => array(
    "required" => 0 ,
    "value_expected" => 1 ,
    "default_val" => null ,
    "help_alias" => "Required for mode seed.
                     Schema with example of desired table structure"
    ),
  "socket" => array(
    "required" => 0 ,
    "value_expected" => 1 ,
    "default_val" => null ,
    "help_alias" => "mysqld socket file (default is to run on all)"
    ),
  "dbname" => array(
    "required" => 0 ,
    "value_expected" => 1 ,
    "default_val" => null ,
    "help_alias" => "Run on named database
                     Default is all not like test, mysql, localinfo,
                     snapshot%, %_restored"
    ),
  "skip_fk_check" => array(
    "required" => 0 ,
    "value_expected" => 0 ,
    "default_val" => null ,
    "help_alias" => "Skip foreign key check (not advisable outside udb)"
    ),
  "skip_trigger_check" => array(
    "required" => 0 ,
    "value_expected" => 0 ,
    "default_val" => null ,
    "help_alias" => "Skip trigger check (not advisable)"
    ),
  "eliminate_dups" => array(
    "required" => 0 ,
    "value_expected" => 0 ,
    "default_val" => null ,
    "help_alias" => "Removes duplicate entries for PK/uniques.
                     Dangerous if run on slaves before masters."
    ),
  "eliminate_unused_columns" => array(
    "required" => 0 ,
    "value_expected" => 0 ,
    "default_val" => null ,
    "help_alias" => "Allows a column to be dropped if it is not
                     the in the new schema"
    ),
  "use_new_pk" => array(
    "required" => 0 ,
    "value_expected" => 0 ,
    "default_val" => null ,
    "help_alias" => "Use new tables PK for doing merging of data.
                     This option will use more diskspace and be slower."
     ),
  "create_missing_table" => array(
    "required" => 0 ,
    "value_expected" => 0 ,
    "default_val" => 0 ,
    "help_alias" => "If this option is set the script will create
                     a missing table"
    ),
  "ignore_partition_differences" => array(
    "required" => 0 ,
    "value_expected" => 0 ,
    "default_val" => 0 ,
    "help_alias" => "If this option is set the script will not consider
                     differences in partitions in its view of whether a table
                     is already in the desired state"
    ),
  "tmpdir" => array(
    "required" => 0,
    "value_expected" => 1,
    "default_val" => null,
    "help_alias" => "Directory to temporarily store data
                     Default is schema data directory"
    ),
  "verbose" => array(
    "required" => 0 ,
    "value_expected" => 1,
    "default_val" => 1,
    "help_alias" => "A value of 0 only shows errors,
                     1 is the default and shows most interesting information,
                     2 is has more detail than is normally useful,
                     3 is very verbose and can break servers"
    ),
  "long_trx_time" => array(
    "required" => 0,
    "value_expected" => 1,
    "default_val" => 3600,
    "help_alias" => "Do not run OSC if a trx running longer than X exists"
    ),
  "osc_class" => array(
    "required" => 0 ,
    "value_expected" => 1,
    "default_val" => "OnlineSchemaChange",
    "help_alias" => "OnlineSchemaChange class to use instead of default"
    ),
  "connection_limit" => array(
    "required" => 0 ,
    "value_expected" => 1 ,
    "default_val" => CONNECTION_LIMIT ,
    "help_alias" => "Wait to run if more than XXX connection exist
                    DEFAULT is ".CONNECTION_LIMIT." with a 10 minute timeout"
    ),
  "scratch_schema" => array(
    "required" => 0 ,
    "value_expected" => 1,
    "default_val" => "test",
    "help_alias" => "Schema to use instead of test for conversions of ALTERs
                     into CREATE TABLE statements"
    ),

 "accept_mysql_version" => array(
    "required" => 0 ,
    "value_expected" => 0,
    "default_val" => null,
    "help_alias" => "Accept a version of MySQL that has not been white listed
                    in the main OSC code"
    ),
  "safe_compression_version" => array(
    "required" => 0 ,
    "value_expected" => 1,
    "default_val" => "5.1.53",
    "help_alias" => "Strip InnoDB compression from CREATE TABLE statements
                    if mysql is less than this version (default 5.1.53)"
    ),

);

if (!($arg_list=validate_args($validate_arg_array))) {
  gen_help($validate_arg_array);
}

if (auth_check() == false) {
  print "ERROR: You have to run this script as root user\n";
  gen_help($validate_arg_array);
  exit(1);
}

foreach($arg_list as $arg => $value) {
  $$arg = $value;
}

if ($osc_class != "OnlineSchemaChange") {
  require_once('osc_helpers/'.$osc_class.'.php');
}

// get localhost
$op = "/bin/hostname | sed -e 's/\.facebook\.com//'";
$basename = trim(shell_exec($op));

$allowed_modes = array('seed','statement','clean');
$check_mode = array_search($mode, $allowed_modes);
if ($check_mode === false) {
  print "ERROR: --mode must be either 'seed', 'statement' or 'clean'.\n".
  gen_help($validate_arg_array);
  exit(1);
}

if ($mode=='seed' &&
     (!isset($seed_db)  || !isset($seed_host) || !isset($seed_tables) ) ) {
  print "ERROR: Mode seed requires seed_host, seed_db and seed_tables args\n";
  gen_help($validate_arg_array);
  exit(1);
}

if ($mode == 'statement'  && !isset($ddl_file) ) {
  print "ERROR: --ddl_file must set if mode is 'statement'\n";
  exit(1);
}

if (!isset($socket)) {
  // get the mysqld instances running by greping sockets in ps -ef
  $mysql_sockets = sockets_mysql_list();
  if (!$mysql_sockets) {
    print "ERROR: no mysqld sockets found\n";
    exit(1);
  }
}
else {
  $mysql_sockets[] = $socket;
  unset($socket);
}

// what flags to pass to OSC, default to dropping old table
$flags = OSC_FLAGS_DROPTABLE;

// verbose logging is too verbose
if ($verbose < 3) {
  if ($verbose >= 2) {
    print "Setting OSC_FLAGS_DROPTABLE\n";
  }
  $flags |= OSC_FLAGS_COMPACTLOG;
}
if ($verbose >= 2) {
  print "Setting OSC_FLAGS_DROPTABLE\n";
}
if ($use_new_pk) {
  $flags |= OSC_FLAGS_USE_NEW_PK;
  if ($verbose >= 2) {
    print "Setting OSC_FLAGS_USE_NEW_PK\n";
  }
}
if ($eliminate_dups) {
  $flags |= OSC_FLAGS_ELIMINATE_DUPS;
  if ($verbose >= 2) {
    print "Setting OSC_FLAGS_ELIMINATE_DUPS\n";
  }
}
if ($eliminate_unused_columns) {
  $flags |= OSC_FLAGS_DROPCOLUMN;
  if ($verbose >= 2) {
    print "Setting OSC_FLAGS_DROPCOLUMN\n";
  }
}
if ($accept_mysql_version) {
  $flags |= OSC_FLAGS_ACCEPT_VERSION;
  if ($verbose >= 2) {
    print "Setting OSC_FLAGS_ACCEPT_VERSION\n";
  }
}

if ($verbose >= 2) {
  $flags |= OSC_FLAGS_LOG_STDOUT;
  if ($verbose >= 2) {
    print "Setting OSC_FLAGS_LOG_STDOUT\n";
  }
}


// seems like a nice enough place to drop the clean code, might as well not mix
// it in with the code that does alterations
if ($mode == 'clean') {
  global $db_admin_user, $db_admin_pass;

  // kill off any running instances of this script
  $pids = shell_exec("ps axu | grep $this_script | grep -v ". getmypid() .
                     " | grep root | grep -v grep | awk '{print \$2}' " );
  if ($pids) {
    $pids = str_replace("\n"," ",$pids);
    if ($verbose) {
      print "Killing $this_script pids $pids\n";
    }
    shell_exec("/bin/kill $pids");
    sleep(5);
    shell_exec("/bin/kill -9 $pids 2>/dev/null");
  }
  foreach ($mysql_sockets as $socket) {
    if ($verbose) {
      print "Running serverCleanup for socket $socket\n";
    }
    OnlineSchemaChange::serverCleanup($socket,$db_admin_user, $db_admin_pass);
    if ($verbose) {
      print "Done\n";
    }

  }
  exit(0);
}

// make sure this script is not already running
$ps_check = is_running($this_script);
if ($ps_check > 4) {
  print "ERROR: there are already $ps_check instances".
        " of $this_script running.\n";
  exit(1);
}


if ($mode == 'statement'){
  $ddl_list = read_sql($ddl_file);
  if (!$ddl_list) {
    print "ERROR: Schema file $ddl_file cannot be read!\n";
    exit(1);
  }else{
  }

}else if ($mode == 'seed'){
  foreach(explode(',',$seed_tables) as $table){
    $seed_info = show_table_schema($seed_host,$seed_db,$table);

    if (! $seed_info) {
      print "ERROR: Table $table cannot be read from seed host.\n";
      exit(1);
    }
    $ddl_list[] = array('table_name' => $seed_info['name'],
                        'ddl'        => $seed_info['schema'],
                        'mode'       => 'seed');
  }
}


foreach ($mysql_sockets as $socket) {
  $host = $basename.":".$socket;
  unset($db_list);

  if ($verbose) {
    print "Host = ".$host."\n";
  }

  if (isset($conn)) {
    mysql_close ($conn);
  }

  $conn = createConnection($db_admin_user,$db_admin_pass,$socket);
  if (! $conn) {
    print "ERROR: Creation of connection to $host failed\n";
    continue;
  }

  // get some global variables from mysqld
  $mysql_vars = mysqld_global_vars($conn);
  if (!$mysql_vars) {
    print "ERROR: Unable to get global mysqld variables from ".$host."\n";
    continue;
  }

  if (!isset($dbname)) {
    $db_list = prod_db_list("localhost:$socket");
  }
  else {
    $db_list[] = $dbname;
  }
  if (!$db_list) {
    if ($verbose) {
      print "ERROR: No schemas found in instance $host\n";
    }
    continue;
  }

  foreach ($ddl_list as $ddl_instance) {
    $table    = $ddl_instance['table_name'];
    $ddl      = $ddl_instance['ddl'];
    $osc_mode = $ddl_instance['mode'];

    if ($osc_mode == 'create' || $osc_mode == 'seed' ){
      $create = $ddl;
    }

    if ($verbose) {
      print "Table: ".$table."\n";
      print "DDL: ".$ddl."\n";
    }

    // check for FKs and TRIGGERs
    if (!isset($skip_trigger_check)) {
      if ($verbose) {
        print "Running trigger check\n";
      }

      $check = trigger_check($conn,$table,$db_list);
      if (!$check) {
        continue;
      }
    }
    if (!isset($skip_fk_check)) {
      if ($verbose) {
        print "Running fk check\n";
      }

      $check = fk_check($conn,$table,$db_list);
      if (!$check) {
        continue;
      }
    }

    foreach ($db_list as $db) {
      if (! ddl_guard($connection_limit,$conn ) ){
        print "ERROR: Server did not fall below $connection_limit ".
              "connections afer 10 minutes\n ";
        exit(1);
      }

      $result = mysql_select_db($db, $conn);
      if (! $result ) {
        print "Could not set default db to $db. Weird.\n";
        continue;
      }

      if ($mysql_vars['mysql_version'] < $safe_compression_version) {
        $create = rmCompression($create);
      }
      $table_check = show_table_schema('localhost:'.$socket,$db,$table);

      if (!$table_check)  {
        if (!$create_missing_table || $osc_mode == 'alter' ){
          print "ERROR: Table $db.$table does not exist on $db\n";
          continue;
        }
        else {
          if ($verbose) {
            print "Table $db.$table does not exist, creating.\n";
          }

          $result = query($create,$conn);
          if (!$result) {
            exit(1);
          }
          continue;
        }
      }

      if ($osc_mode == 'seed' || $osc_mode == 'create') {
        if ($ignore_partition_differences) {
          $current = rmPartition($table_check['schema']);
          $desired = rmPartition($create);
        }
        else {
          $current = $table_check['schema'];
          $desired = $create;
        }
        if ($current == $desired) {
          if ($verbose) {
            print "Table ".$db.".".$table .
                  " already has correct schema. Skipping\n";
          }
          continue;
        }
        if ($verbose >= 3) {
          print "Table doest not already have the correct schema:\n".
                "current:".var_export($current,1)."\n".
                "desired:".var_export($desired,1)."\n";
        }
      }

      if ($osc_mode == 'alter') {
        $create = alter_to_create($db,$table,$ddl,$conn,
                                  $socket,$scratch_schema);
        if (!$create) {
          print "ERROR: Could not convert an ALTER to a CREATE statement\n".
                "We tried using the $scratch_schema schema to create a \n".
                "CREATE TABLE statement and failed.\n".
                "Perhaps you already have a table of the same name in the \n".
                "$scratch_schema schema? \n";
          continue;
        }
        if ($verbose) {
          print "CREATE statement is $create\n";
        }
      }

      // cleanup just in case
      $result = post_osc_mop($db,$create,$host,$table,
                             $flags,$long_trx_time,$tmpdir,$osc_class);
      if (!$result) {
        print "forceCleanup failed for ".$db."\n". osc_last_error($table)."\n";
      }

      // if mysql_version like 5.1%,
      // flush tables before dropping temp __osc table
      if (preg_match('/^5\.1\..*$/',$mysql_vars['mysql_version'])) {
        $flush = true;
      }
      else {
        $flush = false;
      }

      // see if there is enough space
      $data_free = disk_space_check($conn,$mysql_vars);
      if (!$data_free) {
        print "ERROR: Unable to get free space on ".$socket."\n";
        exit(1);
      }
      $required_space = table_size_check($conn,$db,$table,$mysql_vars);
      if (!$required_space) {
        print "ERROR: Unable to get size for ".
               $db.".".$table." on ".$socket."\n";
        exit(1);
      }

      if ($use_new_pk) {
        $required_space = 2*$required_space;
      }

      // Add on another 10% for safety
      $required_space = $required_space * 1.1;

      if ($required_space > $data_free) {
        print "ERROR: Not enough space to execute change".
              " - ".$required_space." < ".$data_free;
        continue;
      }

      if ($verbose >= 2) {
        print "Disk space required $required_space\n";
        print "Disk space availible $data_free\n";
      }

      if ($verbose) {
        print timestamp()." Starting change ".$db.",".$socket."\n";
      }

      $osc = new $osc_class($socket,
                            $db_admin_user,
                            $db_admin_pass,
                            $db,
                            $table,
                            $create,
                            $tmpdir,
                            $flags,
                            500000,
                            500,
                            $long_trx_time,
                            $backup_user);
      $result = $osc->execute();

      if (!$result){
        print "ERROR: osc failed for ".$db."\n" . osc_last_error($table)."\n";
        if (!empty($osc)) {
          $osc->forceCleanup();
        }
        else {
          print "ERROR: osc exception: ". $e->getMessage() . "\n";
        }
         continue;
      }
      if ($verbose) {
        print timestamp()." Finished change ".$db.",".$socket."\n";
      }
      // cleanup again...
      $result = post_osc_mop($db,$create,$host,$table,
                             $flags,$long_trx_time,$tmpdir,$osc_class);
      if (!$result) {
        print "osc forceCleanup failed on ".$db ."\n".
              osc_last_error($table)."\n";
      }
    }
  } // forach alter
} //foreach socket

if ($verbose) {
  print "Done\n";
}

/////////////////////////////
// Functions
/////////////////////////////

function alter_to_create($db,$table,$ddl,$conn,$socket,$scratch_schema) {
  global $db_admin_user, $db_admin_pass;
  $sql = "SET SQL_LOG_BIN = 0";
  $result = query($sql, $conn);
  if (!$result) {
    return false;
  }

  $sql = "SELECT SCHEMA()";
  $result = query($sql, $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);
  $old_schema = $row[0];

  $result = query("USE $scratch_schema",$conn );
  if (!$result) {
    return false;
  }

  $sql = "CREATE TABLE test.$table LIKE $db.$table";
  $result = query($sql, $conn);
  if (!$result) {
    return false;
  }

  $sql = $ddl;
  $result = query($sql, $conn);

  if (!$result) {
    OnlineSchemaChange::dropTable($table,$conn);
    return false;
  }

  $create_info = show_table_schema('localhost:'.$socket,$scratch_schema,$table);

  OnlineSchemaChange::dropTable($table,$conn);

  $result = query("USE $old_schema", $conn);
  if (!$result) {
    return false;
  }

  return $create_info['schema'];
}

// call osc to cleanup triggers and tmp tables from failed run
function post_osc_mop($db,$alter,$sock,$table,$flags,
                      $long_trx_time,$tmpdir,$osc_class) {
  global $db_admin_user, $db_admin_pass;
  try {
    $osc = new $osc_class($sock, $db_admin_user,
      $db_admin_pass, $db, $table, $alter,
      $tmpdir, $flags, 500000, 500, $long_trx_time);
    return $osc->forceCleanup();
  } catch (Exception $e) {
    if (!empty($osc)) {
      print "OSC forceCleanup() for ".$db.".".$table." finished\n";
    }
    else {
      print "ERROR: post_osc_mop got osc exception: ". $e->getMessage();
    }
  }
}

function osc_last_error($table) {
  $osc_err_log = OSC_LOG_DIR.OSC_LOG_PREFIX.$table.".err";
  $cmd = "/usr/bin/tail -n1 ".$osc_err_log;
  $result = trim(shell_exec($cmd));
  return $result;
}

// read sql in file separated by ';'
function read_sql($infile) {
  $create_list = array();
  $change_info = array();
  $file_content = file_get_contents($infile,false,null);

  // Strip out MySQL style comments
  $file_content = preg_replace('/#.+/','',$file_content);
  $file_content = preg_replace('/--.+/','',$file_content);
  $ddl_full = trim($file_content);

  $ddl_list = preg_split("/;/",$ddl_full);

  foreach ($ddl_list as $ddl) {
    $ddl = trim($ddl);
    if ($ddl == '') {
      continue;
    }

    $res = array();
    if (!preg_match("/^(?P<mode>create|alter) table `?(?P<name>\w+)`?/i",
                    $ddl, $res)) {
      print "ERROR: $infile does not contain ALTER or CREATE statement:\n".
            "$create\n";
      return false;
    }
    if (strtolower($res['mode']) == 'create') {
      $ddl = standardize_create($ddl);
    }

    $change_info[] = array('table_name' => $res['name'] ,
                           'ddl'        => $ddl ,
                           'mode'       => strtolower($res['mode']) );
  }
  return $change_info;
}

// get some mysqld globals
function mysqld_global_vars($conn) {
  // get datadir
  $result = query('SHOW VARIABLES LIKE "datadir"', $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);
  $datadir = $row['Value'];

  // get port
  $result = query("show variables like 'port'", $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);
  $port = $row['Value'];

  // get innodb_data_home_dir
  $result = query("show variables like 'innodb_data_home_dir'", $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);
  $innodb_data_home_dir = $row['Value'];

  // get innodb_file_per_table
  $result = query("show variables like 'innodb_file_per_table'", $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);
  $innodb_file_per_table = $row['Value'];

  // get myqsld version
  $result = query("show variables like 'version'", $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);
  $mysql_version = $row['Value'];
  return array(
    "datadir" => $datadir,
    "innodb_data_home_dir" => $innodb_data_home_dir,
    "innodb_file_per_table" => $innodb_file_per_table,
    "mysql_version" => $mysql_version,
    "mysql_port" => $port
  );
}

// what is the free space on disk
function disk_space_check($conn,$mysql_vars) {
  // which partition needs to have space free
  //
  if ($mysql_vars['innodb_file_per_table'] == 'OFF') {
    $partition = $mysql_vars['innodb_data_home_dir'];
  }
  else {
    $partition = $mysql_vars['datadir'];
  }

  // get relevant partition space free
  $data_size_cmd = "/bin/df -P ".$partition." | grep -v '1024-blocks'";
  $data_size_check = ereg_replace(" +", " ", shell_exec($data_size_cmd));
  $tmp_arr = explode(' ',trim($data_size_check));
  $data_free = intval($tmp_arr[3])*1024;

  return $data_free;
}

function table_size_check($conn,$db,$table,$mysql_vars) {
  // get inoodb internal free space
  $result = query("SHOW TABLE STATUS IN `$db` LIKE '$table'", $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);

  $table_size = intval($row['Data_length']) + intval($row['Index_length']);
  return $table_size;
}

function auth_check() {
  $user = shell_exec('whoami');
  $user = trim($user);
  if ('root' != $user) {
    return false;
  }
  return true;
}


// get list of sockets for running mysqld instances
function sockets_mysql_list() {
  $cmd = "ps -ef | grep 'mysqld' | grep 'socket' | grep -v 'mysqld_safe'";
  $cmd .= " | sed -e 's/^.*--socket=//' | sed -e 's/sock.*$/sock/'";
  $result = trim(shell_exec($cmd));

  $sockets = preg_split("/\n/",$result);
  return $sockets;
}

function fqdn_strip($thishost) {
  $arrayTmp = explode(".facebook.com", $thishost);
  $shortname = $arrayTmp[0];
  return($shortname);
}

// if this script is already running, do not start another instance
function is_running($script_name) {
  $op = "/usr/bin/pgrep -f ".$script_name." | wc -l";
  $output = intval(trim(trim(shell_exec($op)),"\r"));
  return $output;
}

// check for FKs on table
function fk_check($conn,$table,$db_list) {
  $schemas = "'".implode($db_list,"','")."'";

  $query = "SELECT COUNT(*) AS count
   FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
   JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc
     USING (CONSTRAINT_SCHEMA,CONSTRAINT_NAME)
   WHERE kcu.REFERENCED_TABLE_NAME IS NOT NULL AND
   (
     ( kcu.TABLE_NAME='$table' AND
       kcu.TABLE_SCHEMA IN($schemas) )
     OR
     ( kcu.REFERENCED_TABLE_NAME='$table' AND
       kcu.REFERENCED_TABLE_SCHEMA IN($schemas) )
   )";
  $result = query($query, $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);
  if ($row['count'] != 0) {
    print "ERROR: FK found in one or more dbs for table ".$table."\n";
    return false;
  }
  return true;
}

// check for TRIGGERs on table
function trigger_check($conn,$table,$db_list) {
  $schemas = "'".implode($db_list,"','")."'";

  $query = "SELECT COUNT(*) AS count
   FROM INFORMATION_SCHEMA.TRIGGERS
   WHERE TRIGGER_NAME NOT LIKE '\_\_osc%' AND
         EVENT_OBJECT_TABLE='$table' AND
         EVENT_OBJECT_SCHEMA IN($schemas)";

 $result = query($query, $conn);
  if (!$result) {
    return false;
  }
  $row = mysql_fetch_array($result);
  if ($row['count'] != 0) {
    print "ERROR: Triggers found in one or more dbs for table ".$table."\n";
    return false;
  }
  return true;
}

function ddl_guard($connection_count,$conn) {
  global $verbose;
  for($i = 0; $i<60; $i++){
    $res = query("SHOW STATUS LIKE 'Threads_running'", $conn);
    if (!$res) {
      print "Unable to get running threads count from database\n";
      return false;
    }
    $row = mysql_fetch_assoc($res);
    $threads_running = $row['Value'];
    if ($threads_running > $connection_count) {
      print "Instance has $threads_running thread running, which is more ";
      print "than the limit of $connection_count connections. Sleeping\n";
      sleep(10);
    }else {
      if ($verbose >= 2) {
        print "Instance has $threads_running thread running, which is less ";
        print "than the limit of $connection_count connections. \n";
      }

      return true;
    }
  }
  return false;
}

function timestamp() {
  return date("[D M d H:i:s Y]");
}

function createConnection() {
  global $socket,$db_admin_user,$db_admin_pass,$conn;
  print print_r(debug_backtrace(),1);
  $conn = mysql_connect('localhost:'.$socket,
                        $db_admin_user,$db_admin_pass,true);
  if (!$conn) {
    print "Cannot connect to ".$socket."\n".mysql_error()."\n";
    return false;
  }

  $result = mysql_select_db('test', $conn);
  if (!$result) {
    print "Unable to select test db on ".$socket."\n".mysql_error()."\n";
    return false;
  }

  $result = query("SET WAIT_TIMEOUT=28800", $conn);
  if (!$result) {
    return false;
  }

  $result = query("SET sql_log_bin=0", $conn);
  if (!$result) {
    return false;
  }
  return $conn;
}

function query($sql,&$conn) {
  global $conn, $socket;

  if ( !isset($conn) || !mysql_ping($conn)) {
    print "Warning - connection to mysql lost, recreating\n";
    $conn = createConnection();
  }

  $result= @mysql_query($sql,$conn);
  if (!$result) {
    print "Query failure on socket $socket\n";
    print "Query: $sql\n";
    print "ERROR: ".mysql_error($conn)."\n";
    return false;
  }
  return $result;
}

function rmCompression($create) {
  $patterns = '/ ?(ROW_FORMAT=[A-Z]+|KEY_BLOCK_SIZE=[0-9]+)/';
  return preg_replace($patterns, '', $create);
}

// strip out the details of individual partitions
function rmPartition($create) {
  $create = preg_replace('/PARTITION p[0-9]* VALUES .* ENGINE = InnoDB,\n/',
                    '', $create);
  $create = preg_replace(
                    '/\(\s+PARTITION p[0-9]* VALUES .* ENGINE = InnoDB\)\s+/',
                    '', $create);
  return $create;
}
