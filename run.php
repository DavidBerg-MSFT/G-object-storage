#!/usr/bin/php -q
<?php
/**
 * this script performs a test iteration. It utilizes the following exit status 
 * codes
 *   0 Iteration successful
 *   1 Iteration failed
 */
require_once(dirname(__FILE__) . '/api/ObjectStorageController.php');
$status = 0;

if ($controller =& ObjectStorageController::getInstance()) {
  if ($controller->initObjects()) {
    ObjectStorageController::log(sprintf('Got controller reference - starting rampup'), 'run.php', __LINE__);
    while($controller->rampup()) $controller->spacing();
    ObjectStorageController::log(sprintf('rampup complete - starting testing'), 'run.php', __LINE__);
    while($controller->op()) $controller->spacing();
    ObjectStorageController::log(sprintf('testing complete - calculating stats'), 'run.php', __LINE__);
    // print results
  	print("\n\n[results]\n");
    $controller->stats();
  }
  else ObjectStorageController::log(sprintf('Unable to initiate test objects'), 'run.php', __LINE__, TRUE);
}

exit($status);
?>