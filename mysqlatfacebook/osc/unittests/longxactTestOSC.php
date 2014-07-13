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
 starts 2 processes. One that does a sleep and another that does OSC
 to see if OSC is bailing out unless OSC_FLAGS_LONG_XACT_OK is set.

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

// initializes the table by populating 1 row.
function initialize() {
  $conn = get_connection();

  $create = 'CREATE TABLE TestOSC (testkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'PRIMARY KEY (testkey)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';


  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert 1st row
  $insert = "insert into TestOSC(testkey, comment, minicomment) ".
            "values(0, '%s', '%s')";
  $insert = sprintf($insert, 'comment for 1', 'mini 1');
  if  (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }

}

// starts a connection and executes sleep for 120s.
function sleepXact() {

  $conn = get_connection();
  query("select sleep(120)", $conn);

}

// does online schema change
function osc($flags = 0) {
  // do online schema change
  global $user, $pwd, $db, $sock;

  $alter = 'CREATE TABLE TestOSC (testkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'newcolumn varchar(10) NOT NULL DEFAULT "newval" , '.
            'PRIMARY KEY (testkey)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                $flags,
                                50000);
  // we expect this to return false due to long running xact.
  invariant(!$osc->execute(), 'Assertion failed: !$osc->execute()');

  $flags |= OSC_FLAGS_LONG_XACT_OK;
  $osc2 = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                 $alter,
                                 null,
                                 $flags,
                                 50000);
  // we expect this to succeed despite long running xact due to the flag above
  invariant($osc2->execute(), 'Assertion failed: $osc2->execute()');
  echo "OSC is done\n";
  return;
}

// does verification to see if data is same as expected
function verify() {
  $conn = get_connection();

  // verify that newcolumn exists in the table and is populated
  // with default value.
  $query5 = "select min(newcolumn) as minval, max(newcolumn) as maxval ".
            "from TestOSC ";
  $result5 = query($query5, $conn);
  $row5 =  mysql_fetch_assoc($result5);
  invariant($row5['minval'] == 'newval', 'Assertion failed: $row5[\'minval\'] == \'newval\'');
  invariant($row5['maxval'] == 'newval', 'Assertion failed: $row5[\'maxval\'] == \'newval\'');

  // drop table
  query('drop table TestOSC', $conn);

}

function main() {
  $status = 0;
  initialize();

  // start a child to do inserts
  $pid = fb_fork();
  if ($pid == -1) {
    die('could not fork');
  } else if ($pid === 0) {
    sleepXact();
    exit(0);
  }
  // sleep for 45s so that the above connection
  // would have a long running xact of atleast 30s
  sleep(45);
  osc(OSC_FLAGS_DROPTABLE);

  // verification
  verify();
  echo "verification done. waiting for other process to exit.\n";
  pcntl_wait($pid, $status);

}

// default values of parameters.
if ($argc < 3) {
  echo "Usage : php stressTestOSC dbname socket user pwd\n";
  exit(1);
}

$db = $argv[1];
$sock = $argv[2];
$user = ($argc > 3 ? $argv[3] : 'root');
$pwd = ($argc > 4 ? $argv[4] : '');


main();
