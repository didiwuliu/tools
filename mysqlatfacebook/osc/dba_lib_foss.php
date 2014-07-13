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


if(! function_exists('validate_args')) {
function validate_args($arg_list_val) {
  $script_args=array('dummy_arg' => null);
  $my_argv = array();
  for($i=1;$i < $_SERVER['argc'];$i++) {
    if($i <= 3) {
      $firstarg = $_SERVER['argv'][1];
      if(preg_match("/^start$/i" , $firstarg)) {
        if($i == 1) {continue;}
      }
      if(preg_match("/^status|stop$/i" , $firstarg)) {
        return(array('dummy_arg'));
      }
    }
    $arg_list = array();
    $arg_list = explode('=', $_SERVER['argv'][$i]);

    if(count($arg_list) > 2) {
      $tmp_arr = array();
      for($j = 1 ; $j < count($arg_list) ; $j++) {
        $tmp_arr[] = $arg_list[$j];

      }
      $arg_str = implode("=", $tmp_arr);
      $arg_list[1] = $arg_str;
    }

    if (isset($arg_list[0]) && '--' == substr($arg_list[0], 0, 2)) {
      $arg_name = str_replace('-', '_', substr($arg_list[0], 2));
      if (isset($arg_list[1])) {
        $my_argv[$arg_name] = $arg_list[1];
      }
      else {
        $my_argv[$arg_name] = true;
      }
    }
    else {
      $arg_name=$_SERVER['argc'][$i];
      $my_argv[$arg_name] = true;
    }
  }

  $default_args=array();
  foreach($arg_list_val as $arg => $param) {
    if (array_key_exists($arg, $my_argv))
    {
      $script_args[$arg] = $my_argv[$arg];
    }
    if (!(is_array($param))) {
      continue;
    }

    foreach($param as $key => $value) {
      switch($key) {
        case "required":
          #check for required args
          if(($value == 1)  && (!(array_key_exists($arg, $my_argv)))) {
              // return error if not defined
              return false;
          }
          break;
        case "default_val":
          if(!(array_key_exists($arg, $my_argv))) {
            // create a variable called $arg with default value if it wasnt
            // called at command line
            $script_args[$arg]=$value;
          }
          break;
        case "value_expected":
          if($value == 1) {
            if ((array_key_exists($arg, $my_argv)) &&
              ($my_argv[$arg] === true)) {
              return false;
            }
          }
          break;
        case "check_re":

          if ((array_key_exists($arg, $my_argv)) &&
            !(preg_match($value , $my_argv[$arg]))) {
            return false;
          }
          break;
        case "str_delim":
          if($my_argv[$arg]) {
            $arg_array=explode($value , $my_argv[$arg]);
            $script_args[$arg] = $arg_array;
          }
       }
     }
   }

  // return error if unknown arguments passed

  $args_passed = (array_keys($my_argv));

  foreach($args_passed as $arg_passed) {
    if(!(array_key_exists($arg_passed , $arg_list_val))) {
      return false;
    }
  }
  return($script_args);
}
}

if(! function_exists('gen_help')) {
//Generates help output based on an input array
function gen_help($validate_arg_array) {

  $str = "USAGE: $_SERVER[SCRIPT_NAME] \n";
  foreach($validate_arg_array as $arg => $param) {
    if($param['required'] != 1) {$str .= "[ " ;}
    $str .= " --$arg";
    if ($param['value_expected'] === 1) {
      $str .= '=';
    }
    else {$str .= '  -  ';}
    $str .= $param['help_alias'] ;
    if($param['required'] != 1) {$str .= " ]" ;}
      $str .= " \n" ;
  }

  print "$str\n";
  exit;
}
}

if(! function_exists('prod_db_list')) {
// list dbs not in standard list
function prod_db_list($host) {
  global $db_admin_user, $db_admin_pass;
  $db_list = null;
  $db = 'information_schema';
  $conn = mysql_connect($host,$db_admin_user,$db_admin_pass);
  if (!$conn) {
    return false;
  }



  $sql = "SELECT SCHEMA_NAME AS db ";
  $sql .= "FROM INFORMATION_SCHEMA.SCHEMATA ";
  $sql .= "WHERE SCHEMA_NAME NOT IN ";
  $sql .= "('localinfo','test','information_schema','mysql') ";
  $sql .= "AND SCHEMA_NAME NOT LIKE 'snapshot%' ";
  $sql .= "AND SCHEMA_NAME NOT LIKE '%_restored' ";
  $result = mysql_query($sql, $conn);
  if (!$result) {
    return false;
  }
  while($row = mysql_fetch_array($result)) {
    $db_list[] = $row['db'];
  }
  mysql_close($conn);
  return $db_list;
}
}

// return an array with the table name and the create statement,
//  minus auto_increment setting
function show_table_schema($host,$db,$table,$verbose=false) {
  global $db_admin_user, $db_admin_pass;
  $max_tries = 10;
  $counter = 0;
  while ($counter < $max_tries) {
    $conn = mysql_connect($host, $db_admin_user, $db_admin_pass);
    if (!$conn) {
      if ($verbose) {
        echo "Could not connect to $host\n - ".mysql_error()."\n";
      }
      $counter++;
      continue;
    }
    $result = mysql_select_db($db, $conn);
    if (!$result) {
      if ($verbose) {
        echo "Could not select $db on $host\n - ".mysql_error()."\n";
      }
      $counter++;
      continue;
    }

    $sql = "SELECT COUNT(*) AS cnt
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = '$db' AND
                  TABLE_NAME   = '$table'";
    $result = mysql_query($sql, $conn);
    if(!$result){
      if ($verbose) {
        echo "Could not query i_s on $host\n - ".mysql_error()."\n";
      }
      $counter++;
      continue;
    }
    $row = mysql_fetch_array($result);
    if(0 == $row['cnt']){
      if ($verbose) {
        echo "Table $db.$table does not exist on $host\n";
      }
      return false;
    }

    $sql = "SHOW CREATE TABLE ".$table;
    $result = mysql_query($sql, $conn);
    if (!$result) {
      if ($verbose) {
        echo "Could not run '".$sql."' in $db on $host\n - ".mysql_error()."\n";
      }
      $counter++;
      continue;
    }
    else {
      while ($row = mysql_fetch_array($result)) {
        $table_info['name'] = $row['Table'];
        $table_info['schema'] = standardize_create($row['Create Table']);
      }
      return $table_info;
    }
  }
  return false;
}

function standardize_create($create_stm) {
  $replacements = array( '/ default /i'            => ' DEFAULT ',
                         '/PRIMARY KEY  \(/'       => 'PRIMARY KEY (',
                         '/auto_increment/i'       => 'AUTO_INCREMENT',
                         '/\s+AUTO_INCREMENT=\d+/' => '',
                         '/\s+PACK_KEYS=0\d+/'     => '', );
  $create_stm = preg_replace(array_keys($replacements),
                      array_values($replacements),
                      $create_stm);

  preg_match_all('/  KEY.+/',$create_stm,$indexes_tmp);
  if(count($indexes_tmp[0]) > 1){
    $create_stm = preg_replace('/  KEY.+/','',$create_stm);
    $indexes = $indexes_tmp[0];
    $indexes = preg_replace('/,$/', '',$indexes);
    sort($indexes);
    $index_txt=implode(",\n",$indexes);
    $create_stm = preg_replace('/\) ENGINE/i',
                              $index_txt."\n) ENGINE",
                              $create_stm, 1);
    $create_stm = preg_replace("/\n+/","\n",$create_stm);
  }
  return $create_stm;
}

// trim create statement to just the columns / indexes specified
function strip_cols_keys($raw_table,$columns=array(),$indexes=array()) {
  $table = '';
  if (!empty($indexes)) {
    foreach($indexes as $index) {
      if (!preg_match(
        "/.*KEY\s+`$index`\s+\(.*/", $raw_table,$key_column_list)) {
        return false;
      }
      else {
        $key_columns = $key_column_list[0];
        $key_columns = preg_replace(
          "/.*KEY\s+`$index`\s+\(/", "", $key_columns
        );
        $key_columns = preg_replace("/\).*/", "", $key_columns);
        $table .= $key_columns."\n";
      }
    }
  }

  if (!empty($columns)) {
    foreach($columns as $column) {
      if (!preg_match(
        "/(.*)`$column`(.*)/i", $raw_table,$column_list)) {
        return false;
      }
      else {
        $this_column = $column_list[0];
        $table .= $this_column."\n";
      }
    }
  }
  return $table;
}

// output string diff between two tables; input raw create table strings
function diff_tables($table1,$table2) {
  $ary1 = explode("\n",$table1);
  $ary2 = explode("\n",$table2);

  $pad_size = max(array_map('strlen', $ary1)) + 4;
  $pass_ary = array_intersect($ary1,$ary2);

  $diff1 = array_diff($ary1,$pass_ary);
  $diff2 = array_diff($ary2,$pass_ary);

  $output = "";
  $i = 0;
  $j = 0;
  if (count($ary1) > count($ary2)) {
    $lines = count($ary1);
  }
  else {
    $lines = count($ary2);
  }
  if (count($diff1) > count($diff2)) {
    $lines = $lines + count($diff1);
  }
  else {
    $lines = $lines + count($diff2);
  }


  for ($k=0;$k<$lines;$k++) {
    if (!isset($ary1[$i]) && !isset($ary2[$j])) {
      continue;
    }
    if (!isset($ary1[$i])) {
      $ary1[$i] = '';
    }
    if (!isset($ary2[$j])) {
      $ary2[$j] = '';
    }

    if (!isset($diff1[$k]) && !isset($diff2[$k])) {
      $tmp = $k."  ".$ary1[$i];
      $tmp_chars = strlen($tmp);
      $pad = $pad_size - $tmp_chars;
      $tmp = $tmp.str_repeat(' ',$pad).$ary2[$j]."\n";
      if ($ary1[$i] != $ary2[$j]) {
        $pretty = "\033[1;32m".$tmp."\033[0m";
      }
      else {
        $pretty = $tmp;
      }
    }
    elseif (isset($diff1[$k]) && !isset($diff2[$k])) {
      $pretty = "\033[1;31m".$k."  ".$ary1[$i]."\n\033[0m";
      $j--;
    }
    elseif (!isset($diff1[$k]) && isset($diff2[$k])) {
      $tmp = $k;
      $tmp_chars = strlen($tmp);
      $pad = $pad_size - $tmp_chars;
      $pretty = "\033[1;31m".$k.str_repeat(' ',$pad).$ary2[$j]."\n\033[0m";
      $i--;
    }
    elseif (isset($diff1[$k]) && isset($diff2[$k])) {
      $tmp = $k."  ".$ary1[$i];
      $tmp_chars = strlen($tmp);
      $pad = $pad_size - $tmp_chars;
      $pretty = "\033[1;31m".$tmp.str_repeat(' ',$pad).$ary2[$j]."\n\033[0m";
    }
    $j++;
    $i++;
    $output .= $pretty;
  }
  return $output;
}
