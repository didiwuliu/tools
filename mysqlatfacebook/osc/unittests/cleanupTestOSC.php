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

require_once dirname(__FILE__) .'/../OnlineSchemaChange.php';
require_once dirname(__FILE__) .'/oscCommon.php';


/**
 Does multiple OSCs with both OSC_FLAGS_ERRORTEST and OSC_FLAGS_NOCLEANUP.
 They will leave behind a bunch of triggers/tables. Do getCleanupTables()
 and serverCleanup() methods to clean them up.

 */

// wrapper around mysql_query
function query($cmd, $conn, $errorok = false) {
  if (!mysql_ping($conn)) {
    echo "connection went away at ".date('c')."\n";
    $conn = get_connection();
  }

  $retval = mysql_query($cmd, $conn);
  if (!$retval) {
    $error = mysql_error($conn);
    $errno = mysql_errno($conn);
    echo "Failed: ".$cmd." due to error: ".$error."\n";
    if (!$errorok) {
     Throw new Exception($error, $errno);
    }
  }
  return $retval;
}

// wrapper around mysql_connect
function get_connection() {
  global $user, $pwd, $db, $sock;
  $conn = mysql_connect(':'.$sock, $user, $pwd, true);

  if (!$conn) {
    echo 'Connection failed';
    exit();
  }

  $retrycnt = 10;
  // retry usedb a few times
  while (!query('use '.$db, $conn, $retrycnt--));

  return $conn;
}

// gets logarithm to the base 2
function getlogbase2($n) {
  $result = 0;
  while ($n >> 1 > 0) {
    $result++;
    $n = $n >> 1;
  }
  return $result;
}

function cleanup() {
  global $sock, $user, $pwd, $db;
  $conn = get_connection();
  $alter = 'CREATE TABLE TestOSC ( '.
            'n int, logbase2n int, logbase10n int, sqrtn int, cuberootn int,'.
            'payload varchar(100), '.
            'PRIMARY KEY (n)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_FORCE_CLEANUP,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

}


function init($numinserts, $numtables) {

  $conn = get_connection();
  // for now we crate PK on n so that inserts are fast. Later we will change it.
  $create = 'CREATE TABLE TestOSC_0 ( '.
            'n int, logbase2n int, logbase10n int, sqrtn int, cuberootn int,'.
            'payload varchar(100), '.
            'PRIMARY KEY (n)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert $numinserts rows starting at 1
  for ($i = 1; $i <= $numinserts; $i++) {

    $insert = "insert into ".
              "TestOSC_0(logbase10n, logbase2n, cuberootn, sqrtn, n, payload) ".
              "values(%d, %d, %d, %d, %d, '%s')";
    $insert = sprintf($insert, (int)log10($i), getlogbase2($i),
                      (int)pow($i, 1/3), (int)sqrt($i), $i,
                      'payload '.$i);
    if  (!query($insert, $conn, true)) {
      echo 'Unable to insert';
      exit();
    }
  }

  // create bunch of other tables like the one above
  for ($j = 1; $j < $numtables; $j++) {
    $create = sprintf("create table TestOSC_%d like TestOSC_0", $j);
    if  (!query($create, $conn, true)) {
      echo 'Unable to create table '.$j;
      exit();
    }
    $insert = sprintf("insert into TestOSC_%d select * from TestOSC_0", $j);
    if  (!query($insert, $conn, true)) {
      echo 'Unable to insert into table '.$j;
      exit();
    }
  }


  echo "Initialization done\n";

}


function doOSC($numtables) {
  // do schema change
  // cleanup();
  global $sock, $user, $pwd, $db;

  for ($j = 0; $j < $numtables; $j++) {
    OnlineSchemaChange::serverCleanup($sock, $user, $pwd, OSC_FLAGS_DROPTABLE);
    $alter = 'CREATE TABLE TestOSC_%d ( '.
             'n int, logbase2n int, logbase10n int, sqrtn int, cuberootn int,'.
             'payload varchar(100), '.
             'PRIMARY KEY (n)) '.
             'ENGINE=InnoDB DEFAULT CHARSET=latin1';

    $alter = sprintf($alter, $j);
    try {
      // do schema change with errortest/nocleanup options so that we
      // fail and don't cleanup.
      $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC_'.$j,
                                    $alter,
                                    null,
                                    OSC_FLAGS_NOCLEANUP|OSC_FLAGS_ERRORTEST);
      $osc->execute();
    } catch (Exception $e) {
      echo "[".$j."]"."Caught and ignored exception ".$e->getMessage()."\n";
    }
  }

}

function test_cleanup($numtables) {
  global $sock, $user, $pwd, $db;
  $conn = get_connection();

  echo "checking if cleanup needed\n";
  var_dump(OnlineSchemaChange::getCleanupTables($conn));

  mysql_close($conn);

  echo "Doing cleanup\n";
  OnlineSchemaChange::serverCleanup($sock, $user, $pwd, OSC_FLAGS_DROPTABLE);

  echo "verifying\n";
  verify($numtables);

  // drop tables
  $conn = get_connection();

  for ($j = 0; $j < $numtables; $j++) {
    $query = sprintf("drop table TestOSC_%d", $j);
    query($query, $conn);
  }

}


function verify($numtables) {
  $conn = get_connection();
  $checksum = 0;

  // check that all $numtables tables exist and their checksums match
  for ($j = 0; $j < $numtables; $j++) {
    $query = sprintf("checksum table TestOSC_%d", $j);
    $result = query($query, $conn);
    $row =  mysql_fetch_assoc($result);
    invariant($row, 'Assertion failed: $row');
    // no more rows
    invariant(!mysql_fetch_assoc($result), 'Assertion failed: !mysql_fetch_assoc($result)');
    $checksum = $row['Checksum'];
    if (!empty($prevchecksum)) {
        invariant($prevchecksum === $checksum, 'Assertion failed: $prevchecksum === $checksum');
    }

    invariant($prevchecksum = $checksum, 'Assertion failed: $prevchecksum = $checksum');
  }

  echo "All checksums match to ".$checksum."\n";

  // check that no __osc_ stuff left behind
  $q1 = "(select T.table_schema as db, substr(T.table_name, 11) as obj ".
        " from information_schema.tables T ".
        " where T.table_name like '__osc_%')";
  $q2 = "(select T.trigger_schema as db, substr(T.trigger_name, 11) as obj ".
        " from information_schema.triggers T ".
        " where T.trigger_name like '__osc_%')";
  $q = $q1." union distinct ".$q2." order by db, obj";

  $result = query($q, $conn);
  // no rows should exist
  invariant(!mysql_fetch_assoc($result), 'Assertion failed: !mysql_fetch_assoc($result)');
  echo "No OSC triggers/tables left behind\n";

}


// default values of parameters.
if ($argc < 3) {
  echo "Usage : php cleanupTestOSC.php dbname socket user pwd\n";
  exit(1);
}

$db = $argv[1];
$sock = $argv[2];
$user = ($argc > 3 ? $argv[3] : 'root');
$pwd = ($argc > 4 ? $argv[4] : '');

$numinserts = 1000;
$numtables = 200;
init($numinserts, $numtables);
doOSC($numtables);
test_cleanup($numtables);

