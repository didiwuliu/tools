#!/bin/bash

#    Copyright 2011-Present Facebook
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

user=root
pass=password
port=3306
sock="/var/run/mysqld/mysqld-$port.sock";
wrapper='osc_wrapper.php';

echo "Starting unittests for osc_wrapper"
mysql -u$user -p$pass -P$port -S$sock -e "DROP DATABASE IF EXISTS osc_test1;
DROP DATABASE IF EXISTS osc_test2;
DROP DATABASE IF EXISTS test;
CREATE DATABASE osc_test1;
CREATE DATABASE osc_test2;
CREATE DATABASE test;"

echo "CREATE TABLE \`TestOSC\` (
  \`testkey\` int(11) NOT NULL DEFAULT '0',
  \`logtestkey\` int(11) NOT NULL DEFAULT '0',
  \`autoincr\` int(11) NOT NULL AUTO_INCREMENT,
  \`minicomment\` varchar(100) DEFAULT NULL,
  PRIMARY KEY (\`logtestkey\`,\`testkey\`),
  KEY \`auto1\` (\`autoincr\`),
  KEY \`auto2\` (\`testkey\`,\`autoincr\`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1" > ddl_file1
echo "ALTER TABLE TestOSC ADD COLUMN NewAwesomeCol varchar(100) DEFAULT 'Awesome';" > ddl_file2

# Table does not yet exists, so error without create_missing_table option
result=$(sudo php $wrapper --socket=$sock --ddl_file=ddl_file1 --mode=statement --dbname=osc_test1)
if [ $(echo $result | grep ERROR | wc -l) != '1'  ]; then
  echo "Unittest failure for missing table "
  echo $result
  exit
fi
echo "Test 1.1: PASS"

#Make sure we did not create it anyways
result=$(mysql -u$user -p$pass -P$port -S$sock osc_test1 -e 'show create table TestOSC' 2>/dev/null)
if [ $(echo $result | grep CREATE | wc -l) != '0'  ]; then
  echo "Test 1.2: FAIL"
  echo "Unittest failure for missing table"
  echo $result
  exit
fi
echo "Test 1.2: PASS"

# Now create a table for real, should have no errors
result=$(sudo php $wrapper --socket=$sock --ddl_file=ddl_file1 --mode=statement --dbname=osc_test1 --create_missing_table 2>&1 )
if [ $(echo $result | grep ERROR | wc -l) != '0'  ]; then
  echo "Test 2.1: FAIL"
  echo " Unittest failure for create"
  echo $result
  exit
fi
echo "Test 2.1: PASS"

# Make sure the table exists
result=$(mysql -u$user -p$pass -P$port -S$sock osc_test1 -e 'show create table TestOSC' 2>/dev/null )
if [ $(echo $result | grep CREATE | wc -l) != '1'  ]; then
  echo "Test 2.2: FAIL"
  echo "Unittest failure for create"
  echo $result
  exit
fi
echo "Test 2.2: PASS"

# Have the script run against all shards
result=$(sudo php $wrapper --socket=$sock --mode=seed --seed_host=127.0.0.1:$port --seed_db=osc_test1 --seed_tables=TestOSC --create_missing_table 2>&1)
if [ $(echo $result | grep ERROR | wc -l) != '0'  ]; then
  echo "Test 2.3: FAIL"
  echo sudo php $wrapper --mode=seed --seed_host=127.0.0.1:$port --seed_db=osc_test1 --seed_tables=TestOSC --create_missing_table 
  echo "Unittest failure for seed create table on shards"
  echo $result
  exit
fi

# Make sure the table now exists in osc_test2
result=$(mysql -u$user -p$pass -P$port -S$sock osc_test2 -e 'show create table TestOSC' 2>/dev/null )
if [ $(echo $result | grep CREATE | wc -l) != '1'  ]; then
  echo "Test 2.3: FAIL"
  echo $result
  exit
fi
echo "Test 2.3: PASS"


# Try an alter, there should be no errors
result=$(sudo php $wrapper --socket=$sock --ddl_file=ddl_file2 --mode=statement --dbname=osc_test1 2>&1)
if [ $(echo $result | grep ERROR | wc -l) != '0'  ]; then
  echo "Test 3.1: FAIL" 
  echo "Unittest failure for update"
  echo $result
  exit
fi
echo "Test 3.1: PASS"                                                             

# Make sure the column exists
result=$(mysql -u$user -p$pass -P$port -S$sock osc_test1 -e 'show create table TestOSC' 2>/dev/null )
if [ $(echo $result | grep NewAwesomeCol | wc -l) != '1'  ]; then                      
  echo "Test 3.2: FAIL"
  echo "Unittest failure for create"                                            
  echo $result
  exit                                                                          
fi                                                                              
echo "Test 3.2: PASS"                                                             

# Try to revert to original schema without specifying 
# --eliminate_unused_columns flag 
result=$(sudo php $wrapper --socket=$sock --ddl_file=ddl_file1 --mode=statement --dbname=osc_test1 2>&1)
if [ $(echo $result | grep 'A column in the existing table is not in newtable' | wc -l) != '1'  ]; then
  echo "Test 4.1: FAIL"
  echo "Unittest failure for drop column"
  echo $result
  exit
fi
echo "Test 4.1: PASS"

result=$(mysql -u$user -p$pass -P$port -S$sock osc_test1 -e 'show create table TestOSC' 2>/dev/null )
if [ $(echo $result | grep NewAwesomeCol | wc -l) != '1'  ]; then
  echo "Test 4.2: FAIL"
  echo "Unittest failure for drop column"
  echo $result
  exit
fi
echo "Test 4.2: PASS"

# now drop it for real
result=$(sudo php $wrapper --socket=$sock --ddl_file=ddl_file1 --mode=statement --dbname=osc_test1 --eliminate_unused_columns 2>&1)
if [ $(echo $result | grep ERROR | wc -l) != '0'  ]; then
  echo "Test 5.1: FAIL"
  echo "Unittest failure for drop column"
  echo $result
  exit
fi
echo "Test 5.1: PASS"

result=$(mysql -u$user -p$pass -P$port -S$sock osc_test1 -e 'show create table TestOSC' 2>/dev/null )
if [ $(echo $result | grep NewAwesomeCol | wc -l) != '0'  ]; then
  echo "Test 5.2: FAIL"
  echo "Unittest failure for drop column"
  echo $result
  exit
fi
echo "Test 5.2: PASS"

# check seed short circuit functionality on a create
result=$(sudo php $wrapper --socket=$sock --ddl_file=ddl_file1 --mode=statement --dbname=osc_test1 2>&1)
if [ $(echo $result | grep 'already has correct schema. Skipping' | wc -l) != '1'  ]; then
  echo "Test 6: FAIL"
  echo "Unittest failure for seed short circuit with create"
  echo $result
  exit
fi
echo "Test 6: PASS"

# Ok, next we will create another schema and use the first as a seed
mysql -u$user -p$pass -P$port -S$sock -e 'DROP DATABASE IF EXISTS osc_test2; CREATE DATABASE osc_test2'

result=$(sudo php $wrapper --socket=$sock --mode=seed --seed_host=127.0.0.1:$port --seed_db=osc_test1 --seed_tables=TestOSC --dbname=osc_test2 --create_missing_table 2>&1)
if [ $(echo $result | grep 'Table osc_test2.TestOSC does not exist, creating. Done' | wc -l) != '1'  ]; then
  echo "Test 7.1: FAIL"
  echo "Unittest failure for seed create table"
  echo $result
  exit
fi
echo "Test 7.1: PASS"

if [ $(echo $result | grep ERROR | wc -l) != '0'  ]; then
  echo "Test 7.2: FAIL"
  echo "Unittest failure for seed create table"
  echo $result
  exit
fi
echo "Test 7.2: PASS"

result=$(sudo php $wrapper --socket=$sock --mode=seed --seed_host=127.0.0.1:$port --seed_db=osc_test1 --seed_tables=TestOSC --dbname=osc_test2 2>&1)
if [ $(echo $result | grep 'already has correct schema. Skipping' | wc -l) != '1'  ]; then
  echo "Test 7.3: FAIL"
  echo "Unittest failure for seed create table short circuit"
  echo $result
  exit
fi
echo "Test 7.3: PASS"

# Drop a column, then test the alter to add it back
mysql -u$user -p$pass -P$port -S$sock osc_test2 -e 'ALTER TABLE TestOSC DROP minicomment'
result=$(sudo php $wrapper --socket=$sock --mode=seed --seed_host=127.0.0.1:$port --seed_db=osc_test1 --seed_tables=TestOSC --dbname=osc_test2 2>&1)
if [ $(echo $result | grep 'Finished change osc_test2' | wc -l) != '1'  ]; then
  echo "Test 8.1: FAIL"
  echo "Unittest failure for seed alter table"
  echo $result
  exit
fi
echo "Test 8.1: PASS"

# Make sure the column exists
result=$(mysql -u$user -p$pass -P$port -S$sock osc_test2 -e 'show create table TestOSC' 2>/dev/null )
if [ $(echo $result | grep minicomment | wc -l) != '1'  ]; then
  echo "Test 8.2: FAIL"
  echo "Unittest failure for seed alter table"                                            
  echo $result
  exit
fi
echo "Test 8.2: PASS"                                                             

#Test out of range issues
mysql -u$user -p$pass -P$port -S$sock osc_test1 -e "CREATE TABLE t1(i1 BIGINT UNSIGNED, i2 INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (i2) ) ENGINE=INNODB;" 
mysql -u$user -p$pass -P$port -S$sock osc_test1 -e "INSERT INTO t1 SET i1=18446744073709551615;"
echo "CREATE TABLE t1(i1 INT,i2 INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (i2) ) ENGINE=INNODB;" > ddl_file3
result=$(sudo php $wrapper --socket=$sock --mode=statement --dbname=osc_test1 --ddl_file=ddl_file3 2>&1)
if [ $(echo $result | grep 'Error loading data' | wc -l) != '1'  ]; then
  echo "Test 9.1: FAIL"
  echo "Unittest failure for out of range integers"
  echo $result
  exit
fi
echo "Test 9.1: PASS"

mysql -u$user -p$pass -P$port -S$sock osc_test1 -e "CREATE TABLE t2(v varchar(20), i1 INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (i1) ) ENGINE=INNODB;" 
mysql -u$user -p$pass -P$port -S$sock osc_test1 -e "INSERT INTO t2 SET v='0123456789012345';"
echo "CREATE TABLE t2(v varchar(10),i1 INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (i1) ) ENGINE=INNODB;" > ddl_file3
result=$(sudo php $wrapper --socket=$sock --mode=statement --dbname=osc_test1 --ddl_file=ddl_file3 2>&1)
if [ $(echo $result | grep 'Error loading data' | wc -l) != '1'  ]; then
  echo "Test 9.2: FAIL"
  echo "Unittest failure for out of range strings"
  exit
fi
echo "Test 9.2: PASS"

# Test fk
mysql -u$user -p$pass -P$port -S$sock osc_test1 -e '
CREATE TABLE `parent` (
  `id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;
CREATE TABLE `child` (
  `id` int(11) DEFAULT NULL,
  `parent_id` int(11) DEFAULT NULL,
  KEY `par_ind` (`parent_id`),
  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;
'
 
echo 'CREATE TABLE `parent` (
  `id` int(11) NOT NULL,
  `newcol` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB' > ddl_file
result=$(sudo php $wrapper --socket=$sock --mode=statement --dbname=osc_test1 --ddl_file=ddl_file 2>&1)
if [ $(echo $result | grep 'FK found in one or more dbs' | wc -l) != '1'  ]; then
  echo "Test 10.1: FAIL"
  echo "Unittest failure for refferenced fk"
  echo $result
  exit
fi
echo "Test 10.1: PASS"

echo 'CREATE TABLE `child` (
  `id` int(11) DEFAULT NULL,
  `parent_id` int(11) DEFAULT NULL,
  `newcol` int(11) DEFAULT NULL,
  KEY `par_ind` (`parent_id`),
  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB' > ddl_file
result=$(sudo php $wrapper --socket=$sock --mode=statement --dbname=osc_test1 --ddl_file=ddl_file 2>&1)
if [ $(echo $result | grep 'FK found in one or more dbs' | wc -l) != '1'  ]; then
  echo "Test 10.2: FAIL"
  echo "Unittest failure for mysql -u$user -p$pass -P$port -S$sockscading fk"
  echo $result
  exit
fi
echo "Test 10.2: PASS"

mysql -u$user -p$pass -P$port -S$sock osc_test1 -e "DROP TABLE IF EXISTS child"
mysql -u$user -p$pass -P$port -S$sock osc_test1 -e 'CREATE TABLE `child` (
  `id` int(11) DEFAULT NULL,
  `parent_id` int(11) DEFAULT NULL,
  KEY `par_ind` (`parent_id`),
  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB' 

echo 'CREATE TABLE `child` (
  `id` int(11) DEFAULT NULL,
  `parent_id` int(11) DEFAULT NULL,
  `newcol` int(11) DEFAULT NULL,
  KEY `par_ind` (`parent_id`),
  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB' > ddl_file

result=$(sudo php $wrapper --socket=$sock --mode=statement --dbname=osc_test1 --ddl_file=ddl_file 2>&1)
if [ $(echo $result | grep 'ERROR' | wc -l) != '1'  ]; then
  echo "Test 10.3: FAIL"
  echo "Unittest failure for alter table with fk"
  echo $result
  exit
fi
echo "Test 10.3: PASS"

echo 'CREATE TABLE `parent` (
  `id` int(11) NOT NULL,
  `newcol` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB' > ddl_file
result=$(sudo php $wrapper --socket=$sock --mode=statement --dbname=osc_test1 --ddl_file=ddl_file 2>&1)
if [ $(echo $result | grep 'FK found in one or more dbs' | wc -l) != '1'  ]; then
  echo "Test 10.4: FAIL"
  echo "Unittest failure for refferenced fk"
  echo $result
  exit
fi
echo "Test 10.4: PASS"

mysql -u$user -p$pass -P$port -S$sock osc_test1 -e "CREATE TABLE TestOSC2 (
  testkey int(11) NOT NULL DEFAULT '0',
  test int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (testkey)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8; "

mysql -u$user -p$pass -P$port -S$sock osc_test2 -e "CREATE TABLE TestOSC2 (
  testkey int(11) NOT NULL DEFAULT '0',
  test int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (testkey)
) ENGINE=InnoDB DEFAULT CHARSET=latin1; "

result=$(sudo php $wrapper --socket=$sock --mode=seed --seed_host=127.0.0.1:$port --seed_db=osc_test1 --seed_tables=TestOSC2 --dbname=osc_test2 --safe_compression_version=6.0.0 2>&1)
if [ $(echo $result | grep 'already has correct schema. Skipping' | wc -l) != '1'  ]; then
  echo "Test 11.1: FAIL"
  echo "Unittest failure for seed create table with compression"
  echo $result
  exit
fi
echo "Test 11.1: PASS"                                                             

result=$(sudo php $wrapper --socket=$sock --mode=seed --seed_host=127.0.0.1:$port --seed_db=osc_test1 --seed_tables=TestOSC2 --dbname=osc_test2 --safe_compression_version=5.1.0 2>&1)
if [ $(echo $result | grep 'already has correct schema. Skipping' | wc -l) != '0'  ]; then
  echo "Test 11.2: FAIL"
  echo "Unittest failure for seed create table with compression"
  echo $result
  exit
fi
echo "Test 11.2: PASS"                                                             

result=$(mysql -u$user -p$pass -P$port -S$sock osc_test2 -e 'show create table TestOSC2' 2>/dev/null )
if [ $(echo $result | grep COMPRESSED | wc -l) != '1'  ]; then
  echo "Test 11.3: FAIL"
  echo "Unittest failure for create"                                            
  echo $result
  exit
fi
echo "Test 11.3: PASS"                                                             

mysql -u$user -p$pass -P$port -S$sock osc_test1 -e 'DROP table if exists partition_test;
CREATE TABLE `partition_test` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `ad_id` bigint(20) unsigned NOT NULL DEFAULT 0,
  `cluster_start` int(10) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`,`cluster_start`),
  KEY `cluster_start` (`cluster_start`)
) ENGINE=InnoDB AUTO_INCREMENT=29656065 DEFAULT CHARSET=latin1 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
/*!50100 PARTITION BY RANGE (cluster_start)
(PARTITION p6 VALUES LESS THAN (1319785200) ENGINE=InnoDB,
 PARTITION p7 VALUES LESS THAN (1322377200) ENGINE=InnoDB,
 PARTITION p8 VALUES LESS THAN (1324969200) ENGINE=InnoDB) */'

echo "ALTER TABLE partition_test ADD COLUMN NewAwesomeCol varchar(100) DEFAULT 'Awesome';" > ddl_file1

# Try an alter, there should be no errors
result=$(sudo php $wrapper --socket=$sock --ddl_file=ddl_file1 --mode=statement --dbname=osc_test1 2>&1)
if [ $(echo $result | grep ERROR | wc -l) != '0'  ]; then
  echo "Test 12.1: FAIL" 
  echo "Unittest failure for compressed partition"
  echo $result
  exit
fi
echo "Test 12.1: PASS"                                                             

# Make sure the column exists
result=$(mysql -u$user -p$pass -P$port -S$sock osc_test1 -e 'show create table partition_test' 2>/dev/null )
if [ $(echo $result | grep NewAwesomeCol | wc -l) != '1'  ]; then
  echo "Test 12.2: FAIL"
  echo "Unittest failure for compressed partition"                                            
  echo $result
  exit
fi
echo "Test 12.2: PASS"                                                             

echo "CREATE TABLE \`partition_test\` (
  \`id\` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  \`ad_id\` bigint(20) unsigned NOT NULL DEFAULT '0',
  \`cluster_start\` int(10) unsigned NOT NULL DEFAULT '0',
  \`NewAwesomeCol\` varchar(100) DEFAULT 'Awesome',
  PRIMARY KEY (\`id\`,\`cluster_start\`),
  KEY \`cluster_start\` (\`cluster_start\`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8
/*!50100 PARTITION BY RANGE (cluster_start)
(PARTITION p6 VALUES LESS THAN (1319785201) ENGINE = InnoDB,
 PARTITION p7 VALUES LESS THAN (1322377201) ENGINE = InnoDB,
 PARTITION p8 VALUES LESS THAN (1324969201) ENGINE = InnoDB) */" > ddl_file1

# make sure we can --ignore_partition_differences works 
result=$(sudo php $wrapper --socket=$sock --ddl_file=ddl_file1 --mode=statement --dbname=osc_test1 --ignore_partition_differences 2>&1)
if [ $(echo $result | grep 'already has correct schema. Skipping' | wc -l) != '1'  ]; then
  echo "Test 12.3: FAIL"
  echo "Unittest failure for ignoring partitioning differences"
  echo $result
  exit
fi
echo "Test 12.3: PASS"                                                             




rm ddl_file*
