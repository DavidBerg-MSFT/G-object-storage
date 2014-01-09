#!/usr/bin/php -q
<?php
/**
 * this script removes the test container, objects and/or files if needed 
 * following testing. It utilizes the following exit status codes:
 *   0 Initialization successful
 *   1 Unable to initialize storage controller
 *   2 Unable to cleanup test objects
 *   3 Unable to cleanup container
 */
require_once(dirname(__FILE__) . '/api/ObjectStorageController.php');
$status = 0;

ObjectStorageController::log(sprintf('Initializing for test run %d', getenv('bm_run_id')), 'run-cleanup.php', __LINE__);
if ($controller =& ObjectStorageController::getInstance()) {
  ObjectStorageController::log(sprintf('%s storage controller initialized successfully. Cleaning up objects/files', $controller->getApi()), 'run-cleanup.php', __LINE__);
  if ($controller->cleanupObjects()) {
    ObjectStorageController::log(sprintf('Objects/files cleaned up succcessfully'), 'run-cleanup.php', __LINE__);
    if ($controller->cleanupContainer()) ObjectStorageController::log(sprintf('Container %s cleaned up successfully', $controller->getContainer()), 'run-cleanup.php', __LINE__);
    else {
      $status = 3;
      ObjectStorageController::log(sprintf('Unable to clean up container %s', $controller->getContainer()), 'run-cleanup.php', __LINE__, TRUE);
    }
  }
  else {
    $status = 2;
    ObjectStorageController::log(sprintf('Unable to cleanup objects'), 'run-cleanup.php', __LINE__, TRUE);
  }
}
else {
  $status = 1;
  ObjectStorageController::log(sprintf('Unable to get instance of %s storage controller', getenv('bm_param_api')), 'run-cleanup.php', __LINE__, TRUE);
}

exit($status);
?>