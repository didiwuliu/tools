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
 Functional test Online Schema Change (OSC) by creating some tables
 and doing alters using OSC.

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

function test_bad_PK_alter() {
  echo "Testing bad alter PK case\n";
  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'primary key(logtestkey, testkey))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert 1st row
  $insert = "insert into TestOSC(logtestkey, testkey, comment, minicomment) ".
    "values(0, 0, '%s', '%s')";
  $insert = sprintf($insert, 'comment for 1', 'mini 1');
  if  (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }

  // do schema change
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'primary key(testkey))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  global $sock, $user, $pwd, $db;
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant(!$osc->execute(), 'Assertion failed: !$osc->execute()'); // we expect error to occur.

  // verify PK has not changed
  $query = "select * from information_schema.statistics ".
            "where table_name = 'TestOSC' and TABLE_SCHEMA = '%s' ".
            "and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX";
  $query = sprintf($query, $db);
  $result = query($query, $conn);
  $row =  mysql_fetch_assoc($result);
  invariant($row['COLUMN_NAME'] === 'logtestkey', 'Assertion failed: $row[\'COLUMN_NAME\'] === \'logtestkey\'');
  $row = mysql_fetch_assoc($result);
  invariant($row['COLUMN_NAME'] === 'testkey', 'Assertion failed: $row[\'COLUMN_NAME\'] === \'testkey\'');
  invariant(!mysql_fetch_assoc($result), 'Assertion failed: !mysql_fetch_assoc($result)');

  echo "Testing bad alter PK with override\n";

  // now try with OSC_FLAGS_ACCEPTPK
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_ACCEPTPK|OSC_FLAGS_DROPTABLE,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()'); // we expect no error to occur.

  // verify PK has changed
  $query = "select * from information_schema.statistics ".
           "where table_name = 'TestOSC' and TABLE_SCHEMA = '%s' ".
           "and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX";
  $query = sprintf($query, $db);
  $result = query($query, $conn);
  $row =  mysql_fetch_assoc($result);
  invariant($row['COLUMN_NAME'] === 'testkey', 'Assertion failed: $row[\'COLUMN_NAME\'] === \'testkey\'');
  invariant(!mysql_fetch_assoc($result), 'Assertion failed: !mysql_fetch_assoc($result)');

  // drop table
  query('drop table TestOSC', $conn);

}


function test_good_PK_alter() {
  echo "Testing reversing PK columns\n";

  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'primary key(logtestkey, testkey))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert 1st row
  $insert = "insert into TestOSC(logtestkey, testkey, comment, minicomment) ".
    "values(0, 0, '%s', '%s')";
  $insert = sprintf($insert, 'comment for 1', 'mini 1');
  if  (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }

  // do schema change
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'primary key(testkey, logtestkey))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  global $sock, $user, $pwd, $db;
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()'); // we expect no error to occur.
  global $db;
  // verify PK has changed
  $query = "select * from information_schema.statistics ".
            "where table_name = 'TestOSC' and TABLE_SCHEMA = '%s' ".
            "and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX";
  $query = sprintf($query, $db);
  $result = query($query, $conn);
  $row =  mysql_fetch_assoc($result);
  invariant($row['COLUMN_NAME'] === 'testkey', 'Assertion failed: $row[\'COLUMN_NAME\'] === \'testkey\'');
  $row = mysql_fetch_assoc($result);
  invariant($row['COLUMN_NAME'] === 'logtestkey', 'Assertion failed: $row[\'COLUMN_NAME\'] === \'logtestkey\'');
  invariant(!mysql_fetch_assoc($result), 'Assertion failed: !mysql_fetch_assoc($result)');

  echo "Testing adding autoincrement column as PK\n";

  // do another schema change
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'ID INT AUTO_INCREMENT, primary key(ID),'.
            'unique key(logtestkey, testkey))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  global $sock, $user, $pwd, $db;
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()'); // we expect no error to occur.
  global $db;
  // verify PK has changed
  $query = "select * from information_schema.statistics ".
           "where table_name = 'TestOSC' and TABLE_SCHEMA = '%s' ".
           "and INDEX_NAME = 'PRIMARY' order by SEQ_IN_INDEX";
  $query = sprintf($query, $db);
  $result = query($query, $conn);
  $row =  mysql_fetch_assoc($result);
  invariant($row['COLUMN_NAME'] === 'ID', 'Assertion failed: $row[\'COLUMN_NAME\'] === \'ID\'');
  invariant(!mysql_fetch_assoc($result), 'Assertion failed: !mysql_fetch_assoc($result)');

  // drop table
  query('drop table TestOSC', $conn);

}

function test_no_PK() {
  echo "Testing table with no PK\n";
  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert 1st row
  $insert = "insert into TestOSC(logtestkey, testkey, comment, minicomment) ".
            "values(0, 0, '%s', '%s')";
  $insert = sprintf($insert, 'comment for 1', 'mini 1');
  if  (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }

  // keep doubling size of table
  for ($i = 0, $size = 1; $size < 1024*8; $i++,  $size *=2) {
    $insert = "insert into TestOSC(logtestkey, testkey, comment, minicomment) ".
              "select %d, testkey+%d, comment, concat(minicomment, %s) ".
              "from TestOSC";
    $insert = sprintf($insert, $i, $size, $i);
    if  (!query($insert, $conn)) {
      echo 'Unable to insert';
      exit();
    }
  }

  // do schema change
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            "newcolumn varchar(10) NOT NULL DEFAULT 'newval')".
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  global $sock, $user, $pwd, $db;
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant(!$osc->execute(), 'Assertion failed: !$osc->execute()'); // we expect error to occur.

  global $db;
  // verify that newcolumn does not exist in the table
  $query = "select column_name, column_key from information_schema.columns ".
           "where table_name ='TestOSC' and table_schema='%s' ".
           "and column_name = 'newval'";
  $query = sprintf($query, $db);
  $result = query($query, $conn);
  invariant(!mysql_fetch_assoc($result), 'Assertion failed: !mysql_fetch_assoc($result)');

  // do schema change with flag to allow new PK addition
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            "newcolumn varchar(10) NOT NULL DEFAULT 'newval',".
            'primary key (testkey))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE | OSC_FLAGS_USE_NEW_PK,
                                50000);

  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

  // verify that the PK exists in the table
  $query = "select * from information_schema.statistics ".
           "where table_name = 'TestOSC' and TABLE_SCHEMA = '%s' ".
           "and INDEX_NAME = 'PRIMARY'";

  $query = sprintf($query, $db);
  $result = query($query, $conn);
  invariant(mysql_fetch_assoc($result),
            'Assertion failed: mysql_fetch_assoc($result)');

  // try to add a new PK on a newly created column which isn't allowed
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            "newcolumn varchar(10) NOT NULL DEFAULT 'newval',".
            "newcol3 INT,".
            'primary key (newcol3))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE | OSC_FLAGS_USE_NEW_PK,
                                50000);

  invariant(!$osc->execute(), 'Assertion failed: !$osc->execute()');

  // try to switch PKs using the second
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            "newcolumn varchar(10) NOT NULL DEFAULT 'newval',".
            'primary key (logtestkey, testkey))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE | OSC_FLAGS_USE_NEW_PK,
                                50000);

  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

  // verify that the PK exists in the table
  $query = "select * from information_schema.statistics ".
           "where table_name = 'TestOSC' and TABLE_SCHEMA = '%s' ".
           "and INDEX_NAME = 'PRIMARY' and COLUMN_NAME = 'logtestkey'";

  $query = sprintf($query, $db);
  $result = query($query, $conn);
  invariant(mysql_fetch_assoc($result),
            'Assertion failed: mysql_fetch_assoc($result)');

  // try to add a new non-unique PK, it should fail during LOAD
  $drop_pk = "ALTER TABLE TestOSC ".
           "DROP PRIMARY KEY";

  query($drop_pk, $conn);


  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            "newcolumn varchar(10) NOT NULL DEFAULT 'newval',".
            'primary key (minicomment))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE | OSC_FLAGS_USE_NEW_PK,
                                50000);

  invariant(!$osc->execute(), 'Assertion failed: !$osc->execute()');

  // try again eliminating duplicates so it works
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_ELIMINATE_DUPS |
                                OSC_FLAGS_DROPTABLE |
                                OSC_FLAGS_USE_NEW_PK,
                                50000);

  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

  $res = query("SELECT count(*) c from TestOSC", $conn);
  $row = mysql_fetch_assoc($res);
  invariant($row['c'] == 8190, 'Assertion failed: Wrong row count after OSC');

  //Eliminate duplicates on non-pk unique indexes, this will fail.
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int(11), '.
           'comment varchar(2048) DEFAULT NULL, minicomment varchar(100),'.
           "newcolumn varchar(10) NOT NULL DEFAULT 'newval',".
           'PRIMARY KEY (`minicomment`),'.
           'UNIQUE KEY `logtestkey` (`logtestkey`)'.
           ') ENGINE=InnoDB DEFAULT CHARSET=latin1';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE ,
                                50000);
  invariant(!$osc->execute(), 'Assertion failed: $osc->execute()');

  // try again eliminating duplicates so it works
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_ELIMINATE_DUPS |
                                OSC_FLAGS_DROPTABLE ,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

  $res = query("SELECT count(*) c from TestOSC", $conn);
  $row = mysql_fetch_assoc($res);
  invariant($row['c'] == 13, 'Assertion failed: Wrong row count after OSC');

  // drop table
  query('drop table TestOSC', $conn);

}

function test_no_nc_indexes() {
  echo "Testing table with no NC indexes\n";
  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'PRIMARY KEY (logtestkey, testkey)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert 1st row
  $insert = "insert into TestOSC(logtestkey, testkey, comment, minicomment) ".
    "values(0, 0, '%s', '%s')";
  $insert = sprintf($insert, 'comment for 1', 'mini 1');
  if  (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }

  // keep doubling size of table
  for ($i = 0, $size = 1; $size < 1024*8; $i++,  $size *=2) {
    $insert = "insert into TestOSC(logtestkey, testkey, comment, minicomment) ".
              "select %d, testkey+%d, comment, minicomment from TestOSC";
    $insert = sprintf($insert, $i, $size);
    if  (!query($insert, $conn)) {
      echo 'Unable to insert';
      exit();
    }
  }

  // do schema change
  global $sock, $user, $pwd, $db;
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            "newcolumn varchar(10) NOT NULL DEFAULT 'newval', ".
            'PRIMARY KEY (logtestkey, testkey)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

  // verify that newcolumn exists in the table and is populated
  // with default value.
  $query = "select min(newcolumn) as minval, max(newcolumn) as maxval, ".
           "count(*) as rowcount from TestOSC ";
  $result = query($query, $conn);
  $row =  mysql_fetch_assoc($result);
  invariant($row['minval'] == 'newval', 'Assertion failed: $row[\'minval\'] == \'newval\'');
  invariant($row['maxval'] == 'newval', 'Assertion failed: $row[\'maxval\'] == \'newval\'');
  invariant($row['rowcount'] == 8192, 'Assertion failed: $row[\'rowcount\'] == 8192');


  // drop table
  query('drop table TestOSC', $conn);
}


function test_autoincrement_nc_index() {
  echo "Testing table with NC autoincrement index\n";
  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'autoincr int NOT NULL AUTO_INCREMENT, '.
            'minicomment varchar(100), '.
            'PRIMARY KEY (logtestkey, testkey), '.
            'KEY auto1(autoincr), '.
            'KEY auto2(testkey, autoincr)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert some rows starting with 5000 for autoincrement
  for ($i = 0; $i < 8192; $i++) {

    $insert = "insert into ".
              "TestOSC(logtestkey, testkey, autoincr, minicomment) ".
              "values(%d, %d, %d, '%s')";
    $insert = sprintf($insert, getlogbase2($i), $i, $i + 5000,
                      'mini '.$i);
    if  (!query($insert, $conn, true)) {
      echo 'Unable to insert';
      exit();
    }
  }

  // do schema change
  global $sock, $user, $pwd, $db;
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'autoincr int NOT NULL AUTO_INCREMENT, '.
            'minicomment varchar(100), '.
            "newcolumn varchar(10) NOT NULL DEFAULT 'newval',".
            'PRIMARY KEY (logtestkey, testkey), '.
            'KEY auto1(autoincr), '.
            'KEY auto2(testkey, autoincr)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');


  // verify that newcolumn exists in the table and is populated
  // with default value.
  $query = "select min(newcolumn) as minval, max(newcolumn) as maxval, ".
           "min(autoincr) as startcounter, max(autoincr) as endcounter, ".
           "count(*) as rowcount from TestOSC ";
  $result = query($query, $conn);
  $row =  mysql_fetch_assoc($result);
  invariant($row['minval'] == 'newval', 'Assertion failed: $row[\'minval\'] == \'newval\'');
  invariant($row['maxval'] == 'newval', 'Assertion failed: $row[\'maxval\'] == \'newval\'');
  invariant($row['rowcount'] == 8192, 'Assertion failed: $row[\'rowcount\'] == 8192');
  invariant($row['startcounter'] == 5000, 'Assertion failed: $row[\'startcounter\'] == 5000');
  invariant($row['endcounter'] == 5000 + 8191, 'Assertion failed: $row[\'endcounter\'] == 5000 + 8191');

  // drop table
  query('drop table TestOSC', $conn);
}

function test_drop_column() {
  echo "Testing dropping column\n";
  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC2 (testkey int, logtestkey int, '.
            'autoincr int NOT NULL AUTO_INCREMENT, '.
            'a char(100), '.
            'b char(100), '.
            'minicomment varchar(128), '.
            'PRIMARY KEY (autoincr) '.
            ') ENGINE=InnoDB DEFAULT CHARSET=latin1';

  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert some rows starting with 5000 for autoincrement
  for ($i = 0; $i < 8192; $i++) {

    $insert = "insert into ".
              "TestOSC2(a, b, logtestkey, testkey, autoincr, minicomment) ".
              "values('a', 'b', %d, %d, %d, '%s')";
    $insert = sprintf($insert, getlogbase2($i), $i, $i + 5000,
                      'mini '.$i);
    if  (!query($insert, $conn, true)) {
      echo 'Unable to insert';
      exit();
    }
  }
  global $sock, $user, $pwd, $db;

  // try to drop a column which should fail
  $alter = 'CREATE TABLE TestOSC2 (testkey int, logtestkey int, '.
            'autoincr int NOT NULL AUTO_INCREMENT, '.
            'b char(100), '.
            'minicomment varchar(128), '.
            'PRIMARY KEY (autoincr) '.
            ') ENGINE=InnoDB DEFAULT CHARSET=latin1';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC2',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant(!$osc->execute(), 'Assertion failed: !$osc->execute()');

  // try to drop a column while using the OSC_FLAGS_DROPCOLUMN flag
  $alter = 'CREATE TABLE TestOSC2 (testkey int, logtestkey int, '.
            'autoincr int NOT NULL AUTO_INCREMENT, '.
            'b char(100), '.
            'minicomment varchar(128), '.
            'PRIMARY KEY (autoincr) '.
            ') ENGINE=InnoDB DEFAULT CHARSET=latin1';
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC2',
                                $alter,
                                null,
                                OSC_FLAGS_DROPCOLUMN | OSC_FLAGS_DROPTABLE,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

  // verify that the column is gone
  $query = "SELECT * FROM information_schema.columns where ".
           "table_name='TestOSC2' AND table_schema='%s' AND column_name='a'";
  $query = sprintf($query, $db);
  $result = query($query, $conn);
  invariant(mysql_num_rows($result) == 0, 'Assertion failed: Col "a" exists');

  //verify that the data is correct
  $query = "SELECT * FROM TestOSC2 order by autoincr";
  $result = query($query, $conn);
  for ($i = 0; $i > 8192;$i++) {
    $row = mysql_fetch_assoc($result);
    invariant($row['b'] == 'b', 'Assertion failed: data for column: b');
    invariant($row['logtestkey'] == getlogbase2($i),
              'Assertion failed: data for column: logtestkey');
    invariant($row['testkey'] == $i,
              'Assertion failed: data for column: testkey');
    invariant($row['autoincr'] == $i + 5000,
              'Assertion failed: data for column: autoincr');
    invariant($row['minicomment'] == 'mini '.$i,
              'Assertion failed: data for column: minicomment');

  }

  // test dropping the auto increment
  $alter = 'CREATE TABLE TestOSC2 (testkey int, logtestkey int, '.
            'a char(100), '.
            'b char(100), '.
            'minicomment varchar(128), '.
            'primary key (testkey) '.
            ') ENGINE=InnoDB DEFAULT CHARSET=latin1';
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC2',
                                $alter,
                                 null,
                                 OSC_FLAGS_USE_NEW_PK | OSC_FLAGS_DROPTABLE |
                                   OSC_FLAGS_DROPCOLUMN,
                                 50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

  // verify that the column is gone
  $query = "SELECT * FROM information_schema.columns where ".
           "table_name='TestOSC2' AND table_schema='%s' ".
           "AND column_name='autoincr'";
  $query = sprintf($query, $db);
  $result = query($query, $conn);
  invariant(mysql_num_rows($result) == 0,
            'Assertion failed: Col "autoincr" exists');


  // drop table
  query('drop table TestOSC2', $conn);



}

function test_out_of_range_alter() {
  echo "Testing out of range values\n";
  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC1(i1 BIGINT UNSIGNED, '.
            'i2 INT NOT NULL AUTO_INCREMENT, '.
            'PRIMARY KEY (i2) ) ENGINE=INNODB;';

  if (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  $insert = 'INSERT INTO TestOSC1 SET i1=18446744073709551615;';

  if (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }

  $alter = 'CREATE TABLE TestOSC1(i1 INT UNSIGNED, '.
           'i2 INT NOT NULL AUTO_INCREMENT, '.
           'PRIMARY KEY (i2) ) ENGINE=INNODB;';

  global $sock, $user, $pwd, $db;
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC1',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant(!$osc->execute(), 'Assertion failed: !$osc->execute()');

  $create = 'CREATE TABLE TestOSC2(v VARCHAR(20), '.
            'i INT NOT NULL AUTO_INCREMENT, '.
            'PRIMARY KEY (i) ) ENGINE=INNODB;';

  if (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  $insert = 'INSERT INTO TestOSC2 SET v="0123456789012345";';

  if (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }

  $alter = 'CREATE TABLE TestOSC2(v VARCHAR(10), '.
           'i INT NOT NULL AUTO_INCREMENT, '.
           'PRIMARY KEY (i) ) ENGINE=INNODB;';

  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC2',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000);
  invariant(!$osc->execute(), 'Assertion failed: !$osc->execute()');

  query('drop table TestOSC1', $conn);
  query('drop table TestOSC2', $conn);
}

function test_partitioned_alter() {
  echo "Testing partitioned alter\n";
  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC2 (testkey int, logtestkey int, '.
            'autoincr int NOT NULL AUTO_INCREMENT, '.
            'a char(100), '.
            'b char(100), '.
            'minicomment varchar(128), '.
            'PRIMARY KEY (autoincr) '.
            ') ENGINE=InnoDB DEFAULT CHARSET=latin1 '.
            'PARTITION BY RANGE (autoincr) ( '.
            'PARTITION p0 VALUES LESS THAN (6000) , '.
            'PARTITION p1 VALUES LESS THAN (7000) , '.
            'PARTITION p2 VALUES LESS THAN (20000) ) ';



  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert some rows starting with 5000 for autoincrement
  for ($i = 0; $i < 8192; $i++) {

    $insert = "insert into ".
              "TestOSC2(a, b, logtestkey, testkey, autoincr, minicomment) ".
              "values('a', 'b', %d, %d, %d, '%s')";
    $insert = sprintf($insert, getlogbase2($i), $i, $i + 5000,
                      'mini '.$i);
    if  (!query($insert, $conn, true)) {
      echo 'Unable to insert';
      exit();
    }
  }
  global $sock, $user, $pwd, $db;

  // try to add a column
  $alter = 'CREATE TABLE TestOSC2 (testkey int, logtestkey int, '.
            'autoincr int NOT NULL AUTO_INCREMENT, '.
            'a char(100), '.
            'b char(100), '.
            'c char(100), '.
            'minicomment varchar(128), '.
            'PRIMARY KEY (autoincr) '.
            ') ENGINE=InnoDB DEFAULT CHARSET=latin1 '.
            'PARTITION BY RANGE (autoincr) ( '.
            'PARTITION p0 VALUES LESS THAN (6000) , '.
            'PARTITION p1 VALUES LESS THAN (7000) , '.
            'PARTITION p2 VALUES LESS THAN (20000) ) ';

  ob_start();
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC2',
                                $alter,
                                null,
                                OSC_FLAGS_DROPCOLUMN | OSC_FLAGS_DROPTABLE |
                                OSC_FLAGS_LOG_STDOUT,
                                50000);
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');
  $stdout = ob_get_contents();
  ob_end_clean();
  $drop_str="ALTER TABLE `__osc_old_TestOSC2` DROP PARTITION";
  invariant(strpos($stdout,"$stdout `p0`") !== NULL
            , 'Assertion failed: partioned table not correctly dropped');
  invariant(strpos($stdout,"$stdout `p1`") !== NULL
            , 'Assertion failed: partioned table not correctly dropped');

  // verify that the column exists
  $query = "SELECT * FROM information_schema.columns where ".
           "table_name='TestOSC2' AND table_schema='%s' AND column_name='c'";
  $query = sprintf($query, $db);
  $result = query($query, $conn);
  invariant(mysql_num_rows($result) == 1,
            'Assertion failed: Col "a" does not exists');

  // drop table
  query('drop table TestOSC2', $conn);
}


function test_kill_dump() {
  echo "Testing killing of dump like connections\n";

  $pid = fb_fork();
  if ($pid == -1) {
    die('could not fork');
  } else if ($pid === 0) {
    $connfork = get_connection();
    $sql='SELECT sleep(15) /*`TestOSC`*/';
    if(@mysql_query($sql, $connfork)) {
      exit(0); #success means we did not get killed, which is failure
    }
    else {
      exit(1);
    }

  }

  $conn = get_connection();
  $create = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'PRIMARY KEY (logtestkey, testkey)) '.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';

  if  (!query($create, $conn)) {
    echo 'Unable to create table';
    exit();
  }

  // insert 1st row
  $insert = "insert into TestOSC(logtestkey, testkey, comment, minicomment) ".
    "values(0, 0, '%s', '%s')";
  $insert = sprintf($insert, 'comment for 1', 'mini 1');
  if  (!query($insert, $conn)) {
    echo 'Unable to insert';
    exit();
  }


  // do schema change
  $alter = 'CREATE TABLE TestOSC (testkey int, logtestkey int, '.
            'comment varchar(2048), minicomment varchar(100), '.
            'primary key(testkey, logtestkey))'.
            'ENGINE=InnoDB DEFAULT CHARSET=latin1';
  global $sock, $user, $pwd, $db;
  $osc = new OnlineSchemaChange($sock, $user, $pwd, $db, 'TestOSC',
                                $alter,
                                null,
                                OSC_FLAGS_DROPTABLE,
                                50000,
                                500,
                                30,
                                $user);
  // we expect no error to occur.
  invariant($osc->execute(), 'Assertion failed: $osc->execute()');

  pcntl_waitpid ($pid ,$status);
  // we expect an error
  invariant(pcntl_wstopsig($status)==1 ,
            'Assertion failed: pcntl_wstopsig($status)==1 ');
  query('drop table TestOSC', $conn);

}


// default values of parameters.
if ($argc < 3) {
  echo "Usage : php funcTestOSC.php dbname socket user pwd\n";
  exit(1);
}

$db = $argv[1];
$sock = $argv[2];
$user = ($argc > 3 ? $argv[3] : 'root');
$pwd = ($argc > 4 ? $argv[4] : '');
test_no_nc_indexes();
test_autoincrement_nc_index();
test_no_PK();
test_bad_PK_alter();
test_drop_column();
test_good_PK_alter();
test_partitioned_alter();
test_kill_dump();


