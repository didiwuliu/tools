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
 Tests the splitting of load into multiple small loads by  Online Schema Change
 (OSC). Note that when OSC selects stuff into outfile it limits number of rows
 to batchsize supplied. Smaller batchsize means more splitpoints. The where
 clause used for each scan needs to pick up where the previous scan left off,
 and the predicates generated depend upon number of columns in PK. So this
 test tries with multiple PKs as well as different data distributions in
 multi-column PK. The data distribution matters for testing as shown below:

 For a two column Pk (a, b), if first scan scanned till (a1, b1), then 2nd
 scan prediacte would be "a > a1 or a=a1 and b > b1". If there is only one
 value for b for every value of a, then even a predicate "a > a1" would have
 worked. SUch a predicate wouldn't work if there are multiple values of b
 for a=a1 and b1 is not the last value. So it is important to test with
 data sets where there are multiple values of column i for a given value
 of column i-1.

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


function init($numinserts) {

  $conn = get_connection();
  // for now we crate PK on n so that inserts are fast. Later we will change it.
  $create = 'CREATE TABLE TestOSC ( '.
            'n int, logbase2n int, logbase10n int, sqrtn int, cuberootn int,'.
            'payload varchar(100), '.
            'PRIMARY KEY (n)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert 1M rows starting at 1
  for ($i = 1; $i <= $numinserts; $i++) {

    $insert = "insert into ".
              "TestOSC(logbase10n, logbase2n, cuberootn, sqrtn, n, payload) ".
              "values(%d, %d, %d, %d, %d, '%s')";
    $insert = sprintf($insert, (int)log10($i), getlogbase2($i),
                      (int)pow($i, 1/3), (int)sqrt($i), $i,
                      'payload '.$i);
    if  (!query($insert, $conn, true)) {
      echo 'Unable to insert';
      exit();
    }
  }

  echo "Initialization done\n";

}


function rebuild() {
  // do schema change
  // cleanup();
  global $sock, $user, $pwd, $db;
  $conn = get_connection();
  $result = query('show create table TestOSC', $conn);
  $row =  mysql_fetch_assoc($result);
  $alter = $row['Create Table'];
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE|OSC_FLAGS_CHECKSUM,
                                100); // use small batch size
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');
}

function verify($numinserts) {
  $conn = get_connection();

  $query = "select logbase10n, logbase2n, cuberootn, sqrtn, n from TestOSC ".
           "order by n";
  $result = query($query, $conn);
  $rowcount = 0;
  while ($row =  mysql_fetch_assoc($result)) {
    $rowcount++;
    invariant($row['n'] == $rowcount, 'Assertion failed: $row[\'n\'] == $rowcount');
    invariant($row['logbase2n'] == getlogbase2($rowcount), 'Assertion failed: $row[\'logbase2n\'] == getlogbase2($rowcount)');
    invariant($row['logbase10n'] == (int)log10($rowcount), 'Assertion failed: $row[\'logbase10n\'] == (int)log10($rowcount)');
    invariant($row['cuberootn'] == (int)pow($rowcount, 1 / 3), 'Assertion failed: $row[\'cuberootn\'] == (int)pow($rowcount, 1 / 3)');
    invariant($row['sqrtn'] == (int)sqrt($rowcount), 'Assertion failed: $row[\'sqrtn\'] == (int)sqrt($rowcount)');
  }

  invariant($rowcount === $numinserts, 'Assertion failed: $rowcount === $numinserts');

}

function test_multiple_PKs($numinserts) {
  $conn = get_connection();

  // note that this alter is done directly (i.e no OSC)
  $alter1 = "alter table TestOSC drop PRIMARY KEY, ".
            "ADD PRIMARY KEY(logbase10n, cuberootn, n)";
  query($alter1, $conn);

  echo "Doing 1st rebuild\n";
  rebuild();

  echo "Doing verification\n";
  verify($numinserts);

  // note that this alter is done directly (i.e no OSC)
  $alter2 = "alter table TestOSC drop PRIMARY KEY, ".
            "ADD PRIMARY KEY(logbase2n, sqrtn, cuberootn, n)";
  query($alter2, $conn);

  echo "Doing 2nd rebuild\n";
  rebuild();

  echo "Doing verification\n";
  verify($numinserts);

  // note that this alter is done directly (i.e no OSC)
  $alter3 = "alter table TestOSC drop PRIMARY KEY, ".
            "ADD PRIMARY KEY(logbase10n, n)";
  query($alter3, $conn);

  echo "Doing 3rd rebuild\n";

  rebuild();

  echo "Doing verification\n";
  verify($numinserts);

  // note that this alter is done directly (i.e no OSC)
  $alter4 = "alter table TestOSC drop PRIMARY KEY, ".
            "ADD PRIMARY KEY(logbase10n, logbase2n, cuberootn, sqrtn, n)";
  query($alter4, $conn);

  echo "Doing 4th rebuild\n";
  rebuild();

  echo "Doing verification\n";
  verify($numinserts);

  // drop table
  query('drop table TestOSC', $conn);

}



// default values of parameters.
if ($argc < 3) {
  echo "Usage : php splitTestOSC.php dbname socket user pwd\n";
  exit(1);
}

$db = $argv[1];
$sock = $argv[2];
$user = ($argc > 3 ? $argv[3] : 'root');
$pwd = ($argc > 4 ? $argv[4] : '');

$numinserts = 1000000;
init($numinserts);
test_multiple_PKs($numinserts);

