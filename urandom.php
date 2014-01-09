#!/usr/bin/php -q
<?php
/**
 * produces and writes random data to stdout. the first and only argument when 
 * running this script should be the number of bytes to generate. Once this 
 * number of bytes has been written to stdout, the script will stop. This 
 * script uses dd if=/dev/urandom bs=1024 count=32 to generate 32KB of random
 * bytes. The output produced will be repeated random sequences of characters
 * from this until the desired number of bytes has been written to stdout. If
 * the desired number of bytes is <32KB, count will be reduced accordingly
 */
if (isset($argv[1]) && is_numeric($argv[1])) {
  $bytes = $argv[1]*1;
  $bs = 1024;
  $count = 32;
  // reduce count for smaller byte sizes
  if (($bs * $count) > $bytes) $count = ceil($bytes/$bs);
  $urandom = shell_exec(sprintf('dd if=/dev/urandom bs=%d count=%d 2>/dev/null', $bs, $count));
  $rbytes = $bs*$count;
  $printed = 0;
  while($printed < $bytes) {
    if (($printed + $rbytes) < $bytes) {
      print($urandom);
      $printed += $rbytes;
    }
    else {
      print(substr($urandom, 0, $bytes - $printed));
      $printed = $bytes;
    }
  }
}
?>