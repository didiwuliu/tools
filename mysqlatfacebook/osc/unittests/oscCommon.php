<?php
// Copyright 2004-present Facebook. All Rights Reserved.


/**
 To avoid adding dependency from OSC on core/utils, define invariant
 and fb_fork()

 */

function invariant($cond, $info) {
  if (!$cond) {
    Throw new Exception("Assertion $info failed!");
  }
}

function fb_fork() {
  return pcntl_fork();
}

