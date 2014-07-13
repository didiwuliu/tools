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
 Stress tests Online Schema Change (OSC) by spawning 4 child processes for doing
 inserts, deletes, updates and OSC concurrently.
 1.Creates and Initializes a table. $init_inserts rows are populated.
 2.Spawns FOUR child processes to do inserts, updates, deletes and OSC
   concurrently. The range of inserts, deletes and updates is disjoint so that
   they don't affect each other, and the end result is deterministic.
 3.Does verification to see that inserts, deletes, updates and schema change
   have happened as expected

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
  $query = 'use `'.$db.'`';
  while (!query($query, $conn, $retrycnt--));

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

// initializes the table by populating $numinserts rows.
// $numinserts MUST be power of 2.
function initialize($numinserts) {
  $conn = get_connection();

  $create = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'testkey2 int, comment varchar(2048), minicomment varchar(100), '.
            'index (testkey, logtestkey)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert 1st row
  $insert = "insert into ".
            "TestOSC(logtestkey, testkey, testkey2, comment, minicomment) ".
            "values(0, 0, 0, '%s', '%s')";
  $insert = sprintf($insert, 'comment for 1', 'mini 1');
  if  (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }


  // keep doubling size of table
  for ($i = 0, $size = 1; $size < $numinserts; $i++,  $size *= 2) {
    $insert = "insert into ".
              "TestOSC(logtestkey, testkey, testkey2, comment, minicomment) ".
              "select %d, testkey+%d, testkey2+%d, comment, minicomment ".
              "from TestOSC";
    $insert = sprintf($insert, $i, $size, $size);
    if  (!query($insert, $conn)) {
      echo 'Unable to insert';
      exit();
    }
  }

  $index = 'create index ncindex on TestOSC(minicomment)';
  query($index, $conn);

  $index = 'create index ncindex2 on TestOSC(comment)';
  query($index, $conn);

  echo "Initialization Done\n";

}

// used for inserts AFTER initialization phase. These inserts
// happen concurrently with OSC.
function inserts($start, $end) {
  $conn = get_connection();
  $retrycnt = 0;

  // do inserts
  for ($i = $start; $i < $end; $i++) {

    $insert = "insert into ".
              "TestOSC(logtestkey, testkey, testkey2, comment, minicomment) ".
              "values(%d, %d, %d, '%s', '%s')";
    $insert = sprintf($insert, getlogbase2($i), $i, $i, 'comment for'.$i,
                      'mini '.$i);
    if  (!query($insert, $conn, true)) {
      echo "Retrying insert\n";
      $i--;
      $retrycnt++;
      if ($retrycnt > 10) {
        echo 'Too many retries';
        exit(1);
      }

    }
    $inserts_done = $i - $start + 1;
    global $sleep_sec, $sleep_dml;
    if (!empty($sleep_dml) && ($inserts_done % $sleep_dml == 0)) {
      sleep($sleep_sec);
    }

  }

  echo "Inserts Done\n";

}

// used for deletes AFTER initialization phase. These inserts
// happen concurrently with OSC.
function deletes($start, $end) {

  $retrycnt = 0;
  $conn = get_connection();

  // do deletes
  for ($i = $start; $i < $end; $i++) {

    $delete = "delete from TestOSC where logtestkey = %d and testkey = %d ".
              "and testkey2 = %d";
    $delete = sprintf($delete, getlogbase2($i), $i, $i);

    if  (!query($delete, $conn, true)) {
      echo "Retrying delete\n";
      $i--;
      // sleep(1);
      $retrycnt++;
      if ($retrycnt > 10) {
        echo 'Too many retries';
        exit(1);
      }
    }

    $deletes_done = $i - $start + 1;
    global $sleep_sec, $sleep_dml;
    if (!empty($sleep_dml) && ($deletes_done % $sleep_dml == 0)) {
      sleep($sleep_sec);
    }

  }

  echo "Deletes Done\n";

}

// used for updates AFTER initialization phase. These updates
// happen concurrently with OSC.
function updates($start, $end) {
  $retrycnt = 0;
  $conn = get_connection();

  // do updates
  for ($i = $start; $i < $end; $i++) {

    $update = "update TestOSC set comment = concat('updated ', comment), ".
              "minicomment = concat('updated ', minicomment) ".
              "where logtestkey = %d and testkey = %d and testkey2 = %d";
    $update = sprintf($update, getlogbase2($i), $i, $i);

    if  (!query($update, $conn, true)) {
      echo "Retrying update\n";
      // sleep(1);
      $i--;
      $retrycnt++;
      if ($retrycnt > 10) {
        echo 'Too many retries';
        exit(1);
      }

    }

    $updates_done = $i - $start + 1;
    global $sleep_sec, $sleep_dml;
    if (!empty($sleep_dml) && ($updates_done % $sleep_dml == 0)) {
      sleep($sleep_sec);
    }

  }

  echo "Updates Done\n";

}


// does online schema change
function osc($flags = 0) {
  // do online schema change
  $conn = get_connection();

  // craft alter table such that newly added column goes into PK and
  // also ordering of PK columns is changed.
  $alter = 'CREATE TABLE `TestOSC` ( '.
           ' `testkey` int(11) DEFAULT NULL,'.
           ' `logtestkey` int(11) DEFAULT NULL,'.
           ' `testkey2` int(11) DEFAULT NULL,'.
           ' `comment` varchar(2048) DEFAULT NULL,'.
           ' `minicomment` varchar(100) DEFAULT NULL,'.
           ' KEY `testkey` (`testkey`,`logtestkey`),'.
           ' KEY `ncindex` (`minicomment`), '.
           ' KEY `ncindex2` (`comment`(767)),'.
           ' PRIMARY KEY (testkey, logtestkey)'.
           ') ENGINE=InnoDB DEFAULT CHARSET=latin1 ';
echo "Running alter\n";
  global $user, $pwd, $db, $sock;
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                $flags,
                                50000);

  $osc->execute();

}

// does verification to see if data is same as expected
function verify() {
  $conn = get_connection();
  global $init_inserts, $start_ins, $end_ins, $start_del, $end_del,
         $start_upd, $end_upd;

  // verify that rows < $start_del are deleted.
  // verify that insert has added rows till $end_ins.
  $query1 = "select min(testkey2) as mintestkey, max(testkey2) as maxtestkey ".
            "from TestOSC";
  $result1 = query($query1, $conn);
  $row1 =  mysql_fetch_assoc($result1);
  invariant($row1['mintestkey'] == $end_del,
            'Assertion failed: $row1[\'mintestkey\'] == $end_del');
  invariant($row1['maxtestkey'] == $end_ins - 1,
            'Assertion failed: $row1[\'maxtestkey\'] == $end_ins - 1');


  // verify that update has NOT modified anything < start_upd or >= end_upd
  $query2 = "select min(testkey2) as mintestkey, max(testkey2) as maxtestkey ".
            "from TestOSC where comment like 'updated%' ";
  $result2 = query($query2, $conn);
  $row2 =  mysql_fetch_assoc($result2);
  invariant($row2['mintestkey'] == $start_upd,
            'Assertion failed: $row2[\'mintestkey\'] == $start_upd');
  invariant($row2['maxtestkey'] == $end_upd - 1,
            'Assertion failed: $row2[\'maxtestkey\'] == $end_upd - 1');

  // verify total count
  $query3 = "select count(*) as total from TestOSC";
  $result3 = query($query3, $conn);
  $row3 =  mysql_fetch_assoc($result3);
  $inserts = $end_ins - $start_ins;
  $deletes = $end_del - $start_del;
  invariant($row3['total'] == $init_inserts + $inserts - $deletes,
   'Assertion failed: $row3[\'total\'] == $init_inserts + $inserts - $deletes');

  // verify that update has MODIFIED every row in (>=start_upd, <end_upd) range
  $query4 = "select count(*) as total from TestOSC ".
            "where comment not like 'updated%%' ".
            "and testkey2 >= %d and testkey2 < %d ";
  $query4 = sprintf($query4, $start_upd, $end_upd);
  $result4 = query($query4, $conn);
  $row4 =  mysql_fetch_assoc($result4);
  invariant($row4['total'] == 0, 'Assertion failed: $row4[\'total\'] == 0');

  // verify that the new PK exists
  global $db;
  $query5 = "select * from information_schema.statistics ".
            "where table_name = 'TestOSC' and TABLE_SCHEMA = '%s' ".
            "and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX";
  $query5 = sprintf($query5, $db);
  $result5 = query($query5, $conn);
  $row5 =  mysql_fetch_assoc($result5);
  invariant($row5['COLUMN_NAME'] === 'testkey',
            'Assertion failed: $row5[\'COLUMN_NAME\'] === \'testkey\'');
  $row5 = mysql_fetch_assoc($result5);
  invariant($row5['COLUMN_NAME'] === 'logtestkey',
            'Assertion failed: $row5[\'COLUMN_NAME\'] === \'logtestkey\'');

  // drop table
  query('drop table TestOSC', $conn);

}

function main() {
  global $init_inserts, $start_ins, $end_ins, $start_del, $end_del,
         $start_upd, $end_upd;
  $status = 0;
  initialize($init_inserts);

  // start a child to do inserts
  $pid = fb_fork();
  if ($pid == -1) {
    die('could not fork');
  } else if ($pid === 0) {
    inserts($start_ins, $end_ins);
    exit(0);
  }


  // we are the parent if we come here.
  // start a child to do updates
  $pid2 = fb_fork();
  if ($pid2 == -1) {
    die('could not fork');
  } else if ($pid2 === 0) {
    updates($start_upd, $end_upd);
    exit(0);
  }

  // we are the parent if we come here.
  // start a child to do deletes
  $pid3 = fb_fork();
  if ($pid3 == -1) {
    die('could not fork');
  } else if ($pid3 === 0) {
    deletes($start_del, $end_del);
    exit(0);
  }

  // we are the parent if we come here.
  // start a child to do OSC
  $pid4 = fb_fork();
  if ($pid4 == -1) {
    die('could not fork');
  } else if ($pid4 === 0) {
    osc(OSC_FLAGS_DROPTABLE | OSC_FLAGS_USE_NEW_PK);
    // alter();
    exit(0);
  }


  pcntl_wait($pid, $status);
  pcntl_wait($pid2, $status);
  pcntl_wait($pid3, $status);
  pcntl_wait($pid4, $status);

  // verification
  verify();

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

$init_inserts = 1024 * 512;

// IMPORTANT : ranges for insert, update and delete MUST be disjoint, AND
// start deletes at 0 AND start inserts at init_inserts+1 for verification
// to work.
$start_del = 0;
$end_del = 100000;
$start_upd = 300000;
$end_upd = 400000;
$start_ins = $init_inserts + 1;
$end_ins = $start_ins + 100000;

$sleep_sec = 1 ; // #seconds to sleep
$sleep_dml = 2000; // sleep after how many DML.


main();
