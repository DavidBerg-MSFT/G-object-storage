#!/usr/bin/php -q
<?php
/**
 * this script initializes a test run prior to the first iteration. It attempts 
 * to authenticate to the storage platform, create and upload the test files.
 * It utilizes the following exit status codes:
 *   0 Initialization successful
 *   1 Unable to initialize storage controller
 *   2 Unable to initialize container
 *   3 Unable to initialize test objects
 */
require_once(dirname(__FILE__) . '/api/ObjectStorageController.php');
$status = 0;

ObjectStorageController::log(sprintf('Initializing for test run %d', getenv('bm_run_id')), 'run-init.php', __LINE__);
if ($controller =& ObjectStorageController::getInstance()) {
  ObjectStorageController::log(sprintf('%s storage controller initialized successfully. Initializing container %s', $controller->getApi(), $controller->getContainer()), 'run-init.php', __LINE__);
  if ($controller->initContainer()) {
    ObjectStorageController::log(sprintf('Container %s initialized successfully', $controller->getContainer()), 'run-init.php', __LINE__);
    if ($controller->initObjects()) ObjectStorageController::log(sprintf('Test objects initialized successfully'), 'run-init.php', __LINE__);
    else {
      $status = 3;
      ObjectStorageController::log(sprintf('Unable to initialize test objects'), 'run-init.php', __LINE__, TRUE);
    }
  }
  else {
    $status = 2;
    ObjectStorageController::log(sprintf('Unable to initialize container %s', $controller->getContainer()), 'run-init.php', __LINE__, TRUE);
  }
}
else {
  $status = 1;
  ObjectStorageController::log(sprintf('Unable to get instance of %s storage controller', getenv('bm_param_api')), 'run-init.php', __LINE__, TRUE);
}

exit($status);
?>