<?php
/**
 * Abstract class to be extended by service specific APIs. Defines and 
 * partially implements the functionality necessary for testing of an object 
 * storage service. API specific implementations should import and extend this 
 * class. Such implementations reside in sub-directories of 
 * 'object-storage/api'. Names of files contains an implementation should end 
 * with 'ObjectStorageController.php' and the associated class should have the
 * same name.
 */
abstract class ObjectStorageController {
  // content-type for objects
  const CONTENT_TYPE = 'application/octet-stream';
  
  // default values for runtime parameters
  const DEFAULT_CONTAINER = 'chtest{resourceId}';
  const DEFAULT_CONTAINER_WAIT = 3;
  const DEFAULT_MULTIPART_MIN_SEGMENT = 5242880;
  const DEFAULT_NAME = 'test{size}.bin';
  const DEFAULT_TYPE = 'pull';
  const DEFAULT_WORKERS = 1;
  const DEFAULT_WORKERS_INIT = 1;
  // maximum test object size (10GB)
  const MAX_OBJECT_SIZE = 10737418240;
  // the amount of free space to enforce as a ratio of what will be used for testing
  const DISK_FREE_BUFFER = 0.1;
  const DEFAULT_ROUND_PRECISION = 4;
  
  /**
   * Runtime properties - set automatically following instantiation. 
   * Documentation for each is provided in the README
   */
  protected $api_endpoint;
  protected $api_key;
  protected $api_region;
  protected $api_secret;
  protected $api_ssl;
  protected $dns_containers;
  
  // private attributes - may not be accessed by API implementations
  private $api;
  private $cleanup;
  private $container;
  private $container_wait;
  private $continue_errors = array();
  private $duration;
  private $encryption;
  private $insecure;
  private $name;
  // hash mapping each size label to its corresponding remote object.
  // populated by the initObjects method. applies only to pull/download tests
  private $objs = array();
  private $rampup;
  private $randomize;
  private $segment;
  private $segmentBytes;
  private $size;
  // hash mapping each size label to its corresponding number of bytes. 
  // populated by the getInstance singleton method
  private $sizes = array();
  private $spacing;
  private $spacing_max;
  private $spacing_max_relative = FALSE;
  private $spacing_min;
  private $spacing_min_relative = FALSE;
  
  // used to track testing stats
  private $stat_bw = 0;                    // mean aggregate bandwidth (bytes/sec)
  private $stat_bw_median = 0;             // median aggregate bandwidth (bytes/sec)
  private $stat_bw_mbs = 0;                // mean aggregate bandwidth (mb/sec)
  private $stat_bw_mbs_median = 0;         // median aggregate bandwidth (mb/sec)
  private $stat_bw_rstdev = 0;             // relative standard deviation (sample) of aggregate bandwidth metrics
  private $stat_bw_rstdevp = 0;            // relative  standard deviation (population) of aggregate bandwidth metrics
  private $stat_bw_stdev = 0;              // standard deviation (sample) of aggregate bandwidth metrics
  private $stat_bw_stdevp = 0;             // standard deviation (population) of aggregate bandwidth metrics
  private $stat_bw_vals = array();         // used to compute the preceeding 8 metrics
  private $stat_ops = 0;                   // total number of test operations (segmented op counted as 1)
  private $stat_ops_failed = 0;            // failed test operations
  private $stat_ops_failed_ratio = 0;      // ratio of failed to total operations as a percentage
  private $stat_ops_pull = 0;              // pull test operations
  private $stat_ops_push = 0;              // push test operations
  private $stat_ops_secure = 0;            // number of secure/https test operations
  private $stat_ops_size = 0;              // mean size (bytes) of test operations
  private $stat_ops_size_median = 0;       // median size (bytes) of test operations
  private $stat_ops_size_mb = 0;           // mean size (megabytes) of test operations
  private $stat_ops_size_mb_median = 0;    // median size (megabytes) of test operations
  private $stat_ops_size_vals = array();   // used to compute the preceeding 2 metrics
  private $stat_ops_success = 0;           // successful test operations
  private $stat_ops_success_ratio = 0;     // ratio of success to total operations as a percentage
  private $stat_ops_times = array();       // operation times - used to compute relative spacing
  private $stat_requests = 0;              // total number of http requests
  private $stat_requests_failed = 0;       // failed http requests
  private $stat_requests_failed_ratio = 0; // ratio of failed to total requests as a percentage
  private $stat_requests_pull = 0;         // pull test requests
  private $stat_requests_push = 0;         // push test requests
  private $stat_requests_secure = 0;       //number of secure/https test requests
  private $stat_requests_success = 0;      // successful test requests
  private $stat_requests_success_ratio = 0;// ratio of success to total requests as a percentage
  private $stat_segment = 0;               // mean segment size (bytes) - if segmented ops performed
  private $stat_segment_median = 0;        // median segment size (bytes) - if segmented ops performed
  private $stat_segment_mb = 0;            // mean segment size (megabytes) - if segmented ops performed
  private $stat_segment_mb_median = 0;     // median segment size (megabytes) - if segmented ops performed
  private $stat_segment_vals = array();    // used to compute the preceeding 4 metrics
  private $stat_speed = 0;                 // mean worker bandwidth speed (bytes/sec)
  private $stat_speed_median = 0;          // median worker bandwidth speed (bytes/sec)
  private $stat_speed_mbs = 0;             // mean worker bandwidth speed (mb/sec)
  private $stat_speed_mbs_median = 0;      // median worker bandwidth speed (mb/sec)
  private $stat_speed_rstdev = 0;          // relative standard deviation (sample) of bandwidth metrics
  private $stat_speed_rstdevp = 0;         // relative standard deviation (population) of bandwidth metrics
  private $stat_speed_stdev = 0;           // standard deviation (sample) of bandwidth metrics
  private $stat_speed_stdevp = 0;          // standard deviation (population) of bandwidth metrics
  private $stat_speed_vals = array();      // used to compute the preceeding 8 metrics
  private $stat_status_codes = array();    // http status codes returned by the storage service and their frequency (e.g. 200/10; 404/2)
  private $stat_time = 0;                  // total test time including admin, rampup and spacing (secs)
  private $stat_time_admin = 0;            // duration of administrative test operations (secs)
  private $stat_time_ops = 0;              // duration of test operations included in the stats (secs)
  private $stat_time_rampup = 0;           // duration of rampup test operations (secs)
  private $stat_time_spacing = 0;          // duration of spacing between test operations (secs)
  private $stat_transfer = 0;              // total bytes transferred (excluding rampup)
  private $stat_transfer_mb = 0;           // total megabytes transferred (excluding rampup)
  private $stat_transfer_pull = 0;         // bytes transferred for pull operations
  private $stat_transfer_pull_mb = 0;      // megabytes transferred for pull operations
  private $stat_transfer_push = 0;         // bytes transferred for push operations
  private $stat_transfer_push_mb = 0;      // megabytes transferred for push operations
  private $stat_workers = 0;               // mean concurrent workers
  private $stat_workers_median = 0;        // median concurrent workers
  private $stat_workers_vals = array();    // used to compute the preceeding 3 metrics
  private $stat_workers_per_cpu = 0;       // mean concurrent workers per CPU cores (workers/[# CPU cores])
  
  private $storage_class;
  // array of operations that can be performed by the op function. each element 
  // in the array is a hash with the following keys:
  //   size
  //   bytes
  //   type
  private $test_queue = array();
  private $test_queue_pos = 0;
  private $type;
  private $workers;
  private $workers_init;
  
  /**
   * Removes the container if the cleanup runtime parameter was specified and 
   * the container was created during initialization
   * @return boolean
   */
  public final function cleanupContainer() {
    $success = TRUE;
    if ($this->cleanup && file_exists(sprintf('%s/.cleanup-container', getenv('bm_run_dir')))) {
      self::log(sprintf('Attempting to delete container %s', $this->container), 'ObjectStorageController::cleanupContainer', __LINE__);
      if ($success = $this->deleteContainer($this->container)) self::log(sprintf('Container %s deleted successfully', $this->container), 'ObjectStorageController::cleanupContainer', __LINE__);
      else self::log(sprintf('Unable to delete container %s', $this->container), 'ObjectStorageController::cleanupContainer', __LINE__, TRUE);
    }
    else self::log(sprintf('Container %s will not be deleted because it was not created or the cleanup runtime flag was not set', $this->container), 'ObjectStorageController::cleanupContainer', __LINE__);
    return $success;
  }
  
  /**
   * Removes test objects and/or files if the cleanup runtime parameter was 
   * specified and the objects/files were created during initialization
   * @return boolean
   */
  public final function cleanupObjects() {
    $success = TRUE;
    foreach(array('.cleanup-objects', '.test-objects') as $file) {
      if (($this->cleanup || $file != '.cleanup-objects') && file_exists($file = sprintf('%s/%s', getenv('bm_run_dir'), $file))) {
        foreach(file($file) as $object) {
          $object = trim($object);
          if ($this->deleteObject($this->container, $object)) self::log(sprintf('Object %s/%s deleted successfully', $this->container, $object), 'ObjectStorageController::cleanupObjects', __LINE__);
          else {
            self::log(sprintf('Unable to delete object %s/%s', $this->container, $object), 'ObjectStorageController::cleanupObjects', __LINE__, TRUE);
            $success = FALSE;
          }
        }
      }
    }
    return $success;
  }
  
  /**
   * invokes 1 or more http requests using curl, waits until they are 
   * completed, records the stats, and returns the associated results. Return
   * value is a hash containing the following keys:
   *   urls:     ordered array of URLs
   *   request:  ordered array of request headers (lowercase keys)
   *   response: ordered array of response headers (lowercase keys)
   *   results:  ordered array of curl result values - includes the following:
   *             speed:              transfer rate (bytes/sec)
   *             time:               total time for the operation
   *             transfer:           total bytes transferred
   *             url:                actual URL used
   *   status:   ordered array of status codes
   *   lowest_status: the lowest status code returned
   *   highest_status: the highest status code returned
   *   body:     response body (only included when $retBody is TRUE)
   * returns NULL if any of the curl commands fail
   * @param array $requests array defining the http requests to invoke. Each 
   * element in this array is a hash with the following possible keys:
   *   method:  http method (default is GET)
   *   headers: hash defining http headers to append
   *   url:     the URL
   *   input:   optional command to pipe into the curl process as the body
   *   body:    optional string or file to pipe into the curl process as the 
   *            body
   *   range:   optional request byte range
   * @param boolean $retBody if TRUE, the response body will be included in the 
   * return
   * @param boolean $record whether or not to record the corresponding stats
   * @return array
   */
  protected final function curl($requests, $retBody=FALSE, $record=FALSE) {
    global $bm_param_debug;
    $fstart = microtime(TRUE);
    $script = sprintf('%s/%s', getenv('bm_run_dir'), 'curl_script_' . rand());
    $fp = fopen($script, 'w');
    fwrite($fp, "#!/bin/sh\n");
    $ifiles = array();
    $ofiles = array();
    $bfiles = array();
    $result = array('urls' => array(), 'request' => array(), 'response' => array(), 'results' => array(), 'status' => array(), 'lowest_status' => 0, 'highest_status' => 0);
    if ($retBody) $result['body'] = array();
    foreach($requests as $i => $request) {
      if (isset($request['body'])) {
        if (file_exists($request['body'])) $file = $request['body'];
        else {
          $ifiles[$i] = sprintf('%s/%s', getenv('bm_run_dir'), 'curl_input_' . rand());
          $f = fopen($ifiles[$i], 'w');
          fwrite($f, $request['body']);
          fclose($f); 
          $file = $ifiles[$i];
        }
        $request['input'] = 'cat ' . $file;
        $request['headers']['content-length'] = filesize($file);
      }
      if (!isset($request['headers'])) $request['headers'] = array();
      $method = isset($request['method']) ? strtoupper($request['method']) : 'GET';
      $body = '/dev/null';
      if ($retBody) {
        $bfiles[$i] = sprintf('%s/%s', getenv('bm_run_dir'), 'curl_body_' . rand());
        $body = $bfiles[$i];
      }
      $cmd = (isset($request['input']) ? $request['input'] . ' | curl --data-binary @-' : 'curl') . ($method == 'HEAD' ? ' -I' : '') . ' -s -o ' . $body . ' -D - -w "transfer=%{' . ($method == 'GET' ? 'size_download' : 'size_upload') . '}\nspeed=%{' . ($method == 'GET' ? 'speed_download' : 'speed_upload') . '}\ntime=%{time_total}\nurl=%{url_effective}" -X ' . $method . ($this->insecure ? ' --insecure' : '');
      $result['request'][$i] = $request['headers'];
      foreach($request['headers'] as $header => $val) $cmd .= sprintf(' -H "%s: %s"', $header, $val);
      if (isset($request['range'])) $cmd .= ' -r ' . $request['range'];
      $result['urls'][$i] = $request['url'];
      $cmd .= sprintf(' "%s"', $request['url']);
      if ($bm_param_debug) self::log(sprintf('Added curl command: %s', $cmd), 'ObjectStorageController::curl', __LINE__);
      $ofiles[$i] = sprintf('%s/%s', getenv('bm_run_dir'), 'curl_output_' . rand());
      fwrite($fp, sprintf("%s > %s 2>&1 &\n", $cmd, $ofiles[$i]));
    }
    fwrite($fp, "wait\n");
    fclose($fp);
    exec(sprintf('chmod 755 %s', $script));
    self::log(sprintf('Created script %s containing %d curl commands. Executing...', $script, count($requests)), 'ObjectStorageController::curl', __LINE__);
    $start = microtime(TRUE);
    exec($script);
    $curl_time = microtime(TRUE) - $start;
    self::log(sprintf('Execution complete - retrieving results', $script, count($requests)), 'ObjectStorageController::curl', __LINE__);
    foreach(array_keys($requests) as $i) {
      foreach(file($ofiles[$i]) as $line) {
        // status code
        if (preg_match('/HTTP[\S]+\s+([0-9]+)\s/', $line, $m)) {
          $status = $m[1]*1;
          $result['status'][$i] = $status;
          if ($result['lowest_status'] === 0 || $status < $result['lowest_status']) $result['lowest_status'] = $status;
          if ($status > $result['highest_status']) $result['highest_status'] = $status;
        }
        // response header
        else if (preg_match('/^([^:]+):\s+"?([^"]+)"?$/', trim($line), $m)) $result['response'][$i][trim(strtolower($m[1]))] = $m[2];
        // result value
        else if (preg_match('/^([^=]+)=(.*)$/', trim($line), $m)) $result['results'][$i][trim(strtolower($m[1]))] = $m[2];
        // body
        if (isset($bfiles[$i]) && file_exists($bfiles[$i])) {
          $result['body'][$i] = file_get_contents($bfiles[$i]);
          unlink($bfiles[$i]);
        }
      }
      unlink($ofiles[$i]);
    }
    foreach($ifiles as $ifile) unlink($ifile);
    unlink($script);
    self::log(sprintf('Results processed - lowest status %d; highest status %d', $result['lowest_status'], $result['highest_status']), 'ObjectStorageController::curl', __LINE__);    
    if (!$result['highest_status']) {
      self::log(sprintf('curl execution failed'), 'ObjectStorageController::curl', __LINE__, TRUE);
      $result = NULL;
    }
    else if ($bm_param_debug) foreach(array_keys($requests) as $i) self::log(sprintf(' => %s: status: %d; transfer: %d; speed: %f; time: %f', $result['results'][$i]['url'], $result['status'][$i], $result['results'][$i]['transfer'], $result['results'][$i]['speed'], $result['results'][$i]['time']), 'ObjectStorageController::curl', __LINE__);
    // update stats
    if ($record && isset($result['results']) && count($result['results']) && $result['highest_status']) {
      self::log(sprintf('Updating test stats'), 'ObjectStorageController::curl', __LINE__);
      // stat_bw                 => mean aggregate bandwidth (bytes/sec)
      // stat_bw_median          => median aggregate bandwidth (bytes/sec)
      // stat_bw_mbs             => mean aggregate bandwidth (mb/sec)
      // stat_bw_mbs_median      => median aggregate bandwidth (mb/sec)
      // stat_bw_rstdev          => relative standard deviation (sample) of aggregate bandwidth metrics
      // stat_bw_rstdevp         => relative  standard deviation (population) of aggregate bandwidth metrics
      // stat_bw_stdev           => standard deviation (sample) of aggregate bandwidth metrics
      // stat_bw_stdevp          => standard deviation (population) of aggregate bandwidth metrics
      // stat_bw_vals            => used to compute the preceeding 8 metrics
      // stat_ops                => total number of test operations (segmented op counted as 1)
      // stat_ops_failed         => failed test operations
      // stat_ops_failed_ratio   => ratio of failed to total operations as a percentage
      // stat_ops_pull           => pull test operations
      // stat_ops_push           => push test operations
      // stat_ops_secure         => number of secure/https test operations
      // stat_ops_size           => mean size (bytes) of test operations
      // stat_ops_size_median    => median size (bytes) of test operations
      // stat_ops_size_mb        => mean size (megabytes) of test operations
      // stat_ops_size_mb_median => median size (megabytes) of test operations
      // stat_ops_size_vals      => used to compute the preceeding 4 metrics
      // stat_ops_success        => successful test operations
      // stat_ops_success_ratio  => ratio of success to total operations as a percentage
      // stat_ops_times          => operation times - used to compute relative spacing
      // stat_requests           => total number of http requests
      // stat_requests_failed    => failed http requests
      // stat_requests_failed_ratio => ratio of failed to total requests as a percentage
      // stat_requests_pull      => pull test requests
      // stat_requests_push      => push test requests
      // stat_requests_secure    => number of secure/https test requests
      // stat_requests_success   => successful test requests
      // stat_requests_success_ratio => ratio of success to total requests as a percentage
      // stat_segment            => mean segment size (bytes) - if segmented ops performed
      // stat_segment_median     => median segment size (bytes) - if segmented ops performed
      // stat_segment_mb         => mean segment size (megabytes) - if segmented ops performed
      // stat_segment_mb_median  => median segment size (megabytes) - if segmented ops performed
      // stat_segment_vals       => used to compute the preceeding 2 metrics
      // stat_speed              => mean worker bandwidth speed (bytes/sec)
      // stat_speed_median       => median worker bandwidth speed (bytes/sec)
      // stat_speed_mbs          => mean worker bandwidth speed (mb/sec)
      // stat_speed_mbs_median   => median worker bandwidth speed (mb/sec)
      // stat_speed_rstdev       => relative standard deviation (sample) of worker bandwidth metrics
      // stat_speed_rstdevp      => relative standard deviation (population) of worker bandwidth metrics
      // stat_speed_stdev        => standard deviation (sample) of worker bandwidth metrics
      // stat_speed_stdevp       => standard deviation (population) of worker bandwidth metrics
      // stat_speed_vals         => used to compute the preceeding 8 metrics
      // stat_status_codes       => http status codes returned by the storage service and their frequency (e.g. 200/10; 404/2)
      // stat_time               => total test time including admin, rampup and spacing (secs)
      // stat_time_admin         => duration of administrative test operations (secs)
      // stat_time_ops           => duration of test operations included in the stats (secs)
      // stat_time_rampup        => duration of rampup test operations (secs)
      // stat_time_spacing       => duration of spacing between test operations (secs)
      // stat_transfer           => total bytes transferred (excluding rampup)
      // stat_transfer_mb        => total megabytes transferred (excluding rampup)
      // stat_transfer_pull      => bytes transferred for pull operations
      // stat_transfer_pull_mb   => megabytes transferred for pull operations
      // stat_transfer_push      => bytes transferred for push operations
      // stat_transfer_push_mb   => megabytes transferred for push operations
      // stat_workers            => mean concurrent workers
      // stat_workers_median     => median concurrent workers
      // stat_workers_per_cpu    => mean concurrent workers per CPU cores (workers/[# CPU cores])
      // stat_workers_vals       => used to compute the preceeding 3 metrics
      // 
      // == $result['results'] ==
      // speed                   => transfer rate (bytes/sec)
      // time                    => total time for the operation
      // transfer                => total bytes transferred
      // url                     => actual URL used
      $request =& $requests[0];
      $rkeys = array_keys($result['results']);
      $method = isset($request['method']) ? strtoupper($request['method']) : 'GET';
      
      // stat_ops, stat_ops_success, stat_ops_success_ratio, stat_ops_failed, stat_ops_failed_ratio
      $this->stat_ops++;
      $result['highest_status'] < 400 ? $this->stat_ops_success++ : $this->stat_ops_failed++;
      
      // stat_ops_pull, stat_ops_push
      $method == 'GET' ? $this->stat_ops_pull++ : $this->stat_ops_push++;
      
      // stat_status_codes
      foreach($result['status'] as $status) isset($this->stat_status_codes[$status]) ? $this->stat_status_codes[$status]++ : $this->stat_status_codes[$status] = 1;
      
      // stat_requests, stat_requests_failed, stat_requests_pull, 
      // stat_requests_push, stat_requests_secure, stat_requests_success
      foreach($rkeys as $i) {
        $this->stat_requests++;
        $result['highest_status'] < 400 ? $this->stat_requests_success++ : $this->stat_requests_failed++;
        $method == 'GET' ? $this->stat_requests_pull++ : $this->stat_requests_push++;
        if (preg_match('/^https/i', $result['results'][$i]['url'])) $this->stat_requests_secure++;
      }
      
      // stats only included for successful operations
      if ($result['highest_status'] < 400) {
        // stat_ops_size, stat_ops_size_median, stat_ops_size_mb, stat_ops_size_mb_median, stat_ops_size_vals
        $size = 0;
        foreach($rkeys as $i) $size += $result['results'][$i]['transfer'];
        $this->stat_ops_size_vals[] = $size;
        
        // stat_bw, stat_bw_median, stat_bw_mbs, stat_bw_mbs_median, stat_bw_rstdev, stat_bw_rstdevp, stat_bw_stdev, stat_bw_stdevp, stat_bw_vals
        $this->stat_bw_vals[] = $size/$curl_time;
        
        // stat_transfer, stat_transfer_mb, stat_transfer_pull, stat_transfer_pull_mb, stat_transfer_push, stat_transfer_push_mb
        $this->stat_transfer += $size;
        $method == 'GET' ? $this->stat_transfer_pull += $size : $this->stat_transfer_push += $size;
        
        // stat_ops_times
        $this->stat_ops_times[] = $curl_time;
        
        // stat_segment, stat_segment_median, stat_segment_mb, stat_segment_mb_median, stat_segment_vals
        if ($this->segmentBytes) foreach($rkeys as $i) $this->stat_segment_vals[] = $result['results'][$i]['transfer'];
        
        // stat_speed, stat_speed_median, stat_speed_mbs, stat_speed_mbs_median, stat_speed_rstdev, stat_speed_rstdevp, stat_speed_stdev, stat_speed_stdevp, stat_speed_vals
        foreach($rkeys as $i) $this->stat_speed_vals[] = $result['results'][$i]['speed'];
      }
      // stat_ops_secure
      foreach($rkeys as $i) {
        if (preg_match('/^https/i', $result['results'][$i]['url'])) {
          $this->stat_ops_secure++;
          break;
        }
      }
      
      // stat_time_admin
      $this->stat_time_admin += microtime(TRUE) - $fstart - $curl_time;
      
      // stat_time_ops
      $this->stat_time_ops += $curl_time;
      
      // stat_workers, stat_workers_median, stat_workers_vals
      $this->stat_workers_vals[] = count($requests);
      
    }
    return $result;
  }
  
  /**
   * downloads an object containing random data. Returns one of the 
   * following values:
   *   TRUE:  successful
   *   FALSE: failed due to http error
   *   NULL:  curl failure (non service related)
   * @param string $name the name for the object to download
   * @param boolean $record whether or not to record stats for this operation
   * @return boolean
   */
  public final function download($name, $record=TRUE) {
    $success = FALSE;
    $bytes = $this->getSizeCached($name);
    self::log(sprintf('Initiating %d byte download of %s/%s', $bytes, $this->container, $name), 'ObjectStorageController::download', __LINE__);
    if ($record) $start = microtime(TRUE);
    if (($download = $this->initDownload($this->container, $name)) && 
        isset($download['url']) && preg_match('/^http/i', $download['url']) && 
        isset($download['headers']) && is_array($download['headers']) && count($download['headers'])) {
      if ($record) $this->stat_time_admin += microtime(TRUE) - $start;
      $requests = array();
      $concurrency = 1;
      // concurrent range requests for a single object
      if ($this->workers > 1 && $this->segmentBytes && $this->rangeRequestsSupported()) {
        $parts = ceil($bytes/$this->segmentBytes);
        if ($parts > $this->workers) $parts = $this->workers;
        if ($parts > 1) {
          $partSize = round($bytes/$parts);
          $lastPartSize = $bytes - ($partSize * ($parts - 1));
          for($i=1; $i<=$parts; $i++) {
            // determine range
            $start = ($i - 1) * $partSize;
            $stop = $start + ($i == $parts ? $lastPartSize : $partSize) - 1;
            $requests[] = array('headers' => $download['headers'], 'url' => $download['url'], 'range' => sprintf('%d-%d', $start, $stop));
            self::log(sprintf('Added range request %d [%d-%d] for %s/%s', $i, $start, $stop, $this->container, $name), 'ObjectStorageController::download', __LINE__);
          }
        }
      }
      // concurrent requests for the same object
      else if ($this->workers > 1 && !$this->segmentBytes) {
        for($i=1; $i<=$this->workers; $i++) {
          $requests[] = array('headers' => $download['headers'], 'url' => $download['url']);
          self::log(sprintf('Added concurrent request %d for %s/%s', $i, $this->container, $name), 'ObjectStorageController::download', __LINE__);
        }
      }
      // single request
      if (!count($requests)) {
        $requests[] = array('headers' => $download['headers'], 'url' => $download['url']);
        self::log(sprintf('Added single request for %s/%s', $this->container, $name), 'ObjectStorageController::download', __LINE__);
      }
      
      if ($result = $this->curl($requests, FALSE, $record)) {
        if ($result['highest_status'] < 400) {
          $success = TRUE;
          self::log(sprintf('Successfully completed %d download requests for %s/%s', count($requests), $this->container, $name), 'ObjectStorageController::download', __LINE__);
        }
        else {
          $success = $result['lowest_status'] && ($result['lowest_status'] < 400 || in_array($result['lowest_status'], $this->continue_errors)) ? FALSE : NULL;
          self::log(sprintf('One or more download requests for %s/%s failed - lowest status %d; highest status %d', $this->container, $name, $result['lowest_status'], $result['highest_status']), 'ObjectStorageController::download', __LINE__, TRUE);
        }
      }
      else self::log(sprintf('curl download execution for %s/%s failed', $this->container, $name), 'ObjectStorageController::download', __LINE__, TRUE);
    }
    else self::log(sprintf('Unable to initiate download for %s/%s', $this->container, $name), 'ObjectStorageController::download', __LINE__, TRUE);
    return $success;
  }
  
  /**
   * Returns the name of the API
   * @return string
   */
  public final function getApi() {
    return $this->api;
  }
  
  /**
   * internal method used to determine the size of objects prior to downloading
   * @param string $name the object to return the size for
   * @return int
   */
  private final function getSizeCached($name) {
    if (!isset($this->sizeCache)) $this->sizeCache = array();
    if (!isset($this->sizeCache[$name])) $this->sizeCache[$name] = $this->getObjectSize($this->container, $name);
    return isset($this->sizeCache[$name]) ? $this->sizeCache[$name] : NULL;
  }
  
  /**
   * Returns the name of the object storage container
   * @return string
   */
  public final function getContainer() {
    return $this->container;
  }
  
  /**
   * Singleton method to use in order to instantiate
   * @return ObjectStorageController
   */
  public static final function &getInstance() {
    global $base_error_level;
    static $_instances;
    $api = getenv('bm_param_api');
    
    // set some global settings/variables
    if (!ini_get('date.timezone')) ini_set('date.timezone', ($tz = trim(shell_exec('date +%Z'))) ? $tz : 'UTC');
    if (!isset($base_error_level)) $base_error_level = error_reporting();
    
    if (!isset($_instances[$api])) {
      $dir = dirname(__FILE__) . '/' . $api;
      if ($api && is_dir($dir)) {
        $d = dir($dir);
        $controller_file = NULL;
        while($file = $d->read()) {
          if (preg_match('/ObjectStorageController\.php$/', $file)) $controller_file = $file;
        }
        if ($controller_file) {
          require_once($dir . '/' . $controller_file);
          $controller_class = str_replace('.php', '', basename($controller_file));
          if (class_exists($controller_class)) {
            self::log(sprintf('Instantiating new ObjectStorageController using class %s', $controller_class), 'ObjectStorageController::getInstance', __LINE__);
            $_instances[$api] = new $controller_class();
            if (is_subclass_of($_instances[$api], 'ObjectStorageController')) {
              // set runtime parameters
              $_instances[$api]->api = $api;
              $_instances[$api]->api_endpoint = getenv('bm_param_api_endpoint');
              $_instances[$api]->api_key = getenv('bm_param_api_key');
              $_instances[$api]->api_region = getenv('bm_param_api_region');
              $_instances[$api]->api_secret = getenv('bm_param_api_secret');
              $_instances[$api]->api_ssl = getenv('bm_param_api_ssl') == '1';
              $_instances[$api]->cleanup = getenv('bm_param_cleanup') === NULL || getenv('bm_param_cleanup') == '1';
              $_instances[$api]->container = getenv('bm_param_container');
              if (!$_instances[$api]->container) $_instances[$api]->container = self::DEFAULT_CONTAINER;
              $_instances[$api]->container_wait = getenv('bm_param_container_wait');
              if ($_instances[$api]->container_wait === NULL || !is_numeric($_instances[$api]->container_wait) || $_instances[$api]->container_wait < 0) $_instances[$api]->container_wait = self::DEFAULT_CONTAINER_WAIT;
              self::log(sprintf('Base container name %s; container_wait: %d; Resource ID %d', $_instances[$api]->container, $_instances[$api]->container_wait, getenv('bm_resource_id')), 'ObjectStorageController::getInstance', __LINE__);
              $_instances[$api]->container = str_replace('{resourceId}', getenv('bm_resource_id'), $_instances[$api]->container);
              self::log(sprintf('Actual container name: %s', $_instances[$api]->container), 'ObjectStorageController::getInstance', __LINE__);
              if (getenv('bm_param_continue_errors')) {
                foreach(explode(',', getenv('bm_param_continue_errors')) as $status) {
                  if (is_numeric($status = trim($status)) && $status >= 400 && $status < 600 && !in_array($status, $_instances[$api]->continue_errors)) $_instances[$api]->continue_errors[] = $status*1;
                }
              } 
              if ($_instances[$api]->continue_errors) self::log(sprintf('The following error status codes will be ignored: ', implode(', ', $_instances[$api]->continue_errors)), 'ObjectStorageController::getInstance', __LINE__);
              $_instances[$api]->dns_containers = getenv('bm_param_dns_containers') !== '0';
              $_instances[$api]->duration = getenv('bm_param_duration');
              if (preg_match('/^([0-9]+)([smh])$/i', trim($_instances[$api]->duration), $m)) {
                $multiplier = strtolower($m[2]) == 'm' ? 60 : (strtolower($m[2]) == 'h' ? 3600 : 1);
                $_instances[$api]->duration = $m[1] * $multiplier;
              }
              else $_instances[$api]->duration *= 1;
              self::log(sprintf('Set test duration to %d secs', $_instances[$api]->duration), 'ObjectStorageController::getInstance', __LINE__);
              $_instances[$api]->encryption = getenv('bm_param_encryption');
              $_instances[$api]->insecure = getenv('bm_param_insecure') == '1';
              $_instances[$api]->name = getenv('bm_param_name');
              if (!$_instances[$api]->name) $_instances[$api]->name = self::DEFAULT_NAME;
              $_instances[$api]->name = str_replace('{resourceId}', getenv('bm_resource_id'), $_instances[$api]->name);
              $_instances[$api]->rampup = getenv('bm_param_rampup');
              if (preg_match('/^([0-9]+)([smh])$/i', trim($_instances[$api]->rampup), $m)) {
                $multiplier = strtolower($m[2]) == 'm' ? 60 : (strtolower($m[2]) == 'h' ? 3600 : 1);
                $_instances[$api]->rampup = $m[1] * $multiplier;
              }
              else $_instances[$api]->rampup *= 1;
              self::log(sprintf('Set rampup to %d secs', $_instances[$api]->rampup), 'ObjectStorageController::getInstance', __LINE__);
              $_instances[$api]->randomize = getenv('bm_param_randomize') == '1';
              $_instances[$api]->segment = getenv('bm_param_segment');
              if ($_instances[$api]->segment == '1') $_instances[$api]->segment = round((($_instances[$api]->multipartMinSegment() ? $_instances[$api]->multipartMinSegment() : $_instances[$api]->multipartMaxSegment())/1024)/1024) . 'MB';
              if ($_instances[$api]->segment) $_instances[$api]->segmentBytes = self::sizeToBytes($_instances[$api]->segment);
              $_instances[$api]->size = getenv('bm_param_size');
              // convert size labels to normalized byte values
              foreach(explode(',', $_instances[$api]->size) as $size) {
                if ($bytes = self::sizeToBytes($size)) {
                  self::log(sprintf('Translated size %s to %d bytes', $size, $bytes), 'ObjectStorageController::getInstance', __LINE__);
                  $_instances[$api]->sizes[$size] = $bytes;
                }
                else self::log(sprintf('%s is not a valid size label - skipping', $size), 'ObjectStorageController::getInstance', __LINE__, TRUE);
              }
              $_instances[$api]->spacing = getenv('bm_param_spacing');
              if (preg_match('/([0-9]+[ms%]?)+\s*\-?\s*([0-9]+[ms%]?)?/', $_instances[$api]->spacing, $m)) {
          			// convert 'm' and 's' suffux to microseconds
          			for($i=1; $i<count($m); $i++) {
          				if (preg_match('/([0-9]+)([ms%])/', $m[$i], $p)) {
          					$m[$i] = $p[1] * ($p[2] == 's' ? 1000000 : ($p[2] == '%' ? 1 : 1000));
          					if ($p[2] == '%') $i == 1 ? $_instances[$api]->spacing_min_relative = TRUE : $_instances[$api]->spacing_max_relative = TRUE;
          				}
          				else $m[$i] *= 1;
          			}
          			$_instances[$api]->spacing_min = $m[1];
          			if (isset($m[2])) $_instances[$api]->spacing_max = $m[2];
              }
              else {
                $_instances[$api]->spacing *= 1;
                $_instances[$api]->spacing_max = $_instances[$api]->spacing;
                $_instances[$api]->spacing_min = $_instances[$api]->spacing;
              }
              self::log(sprintf('Set spacing parameters from %s. spacing_min: %d; spacing_min_relative: %d; spacing_max: %d; spacing_max_relative: %d', $_instances[$api]->spacing, $_instances[$api]->spacing_min, $_instances[$api]->spacing_min_relative, $_instances[$api]->spacing_max, $_instances[$api]->spacing_max_relative), 'ObjectStorageController::getInstance', __LINE__);
              $_instances[$api]->storage_class = getenv('bm_param_storage_class');
              $_instances[$api]->type = strtolower(trim(getenv('bm_param_type')));
              if (!$_instances[$api]->type || ($_instances[$api]->type != 'pull' && $_instances[$api]->type != 'push' && $_instances[$api]->type != 'both')) $_instances[$api]->type = self::DEFAULT_TYPE;
              $_instances[$api]->workers = getenv('bm_param_workers');
              if (preg_match('/^([0-9]+)\s*\/\s*core$/i', trim($_instances[$api]->workers), $m) || preg_match('/^([0-9]+)\s*\/\s*cpu$/i', trim($_instances[$api]->workers), $m)) {
                $_instances[$api]->workers = $m[1] * getenv('bm_cpu_count');
              }
              if (!$_instances[$api]->workers || !is_numeric($_instances[$api]->workers) || $_instances[$api]->workers < 1) $_instances[$api]->workers = self::DEFAULT_WORKERS;
              
              $_instances[$api]->workers_init = getenv('bm_param_workers_init');
              if (preg_match('/^([0-9]+)\s*\/\s*core$/i', trim($_instances[$api]->workers_init), $m) || preg_match('/^([0-9]+)\s*\/\s*cpu$/i', trim($_instances[$api]->workers_init), $m)) {
                $_instances[$api]->workers_init = $m[1] * getenv('bm_cpu_count');
              }
              if (!$_instances[$api]->workers_init || !is_numeric($_instances[$api]->workers_init) || $_instances[$api]->workers_init < 1) $_instances[$api]->workers_init = self::DEFAULT_WORKERS_INIT;
              
              self::log(sprintf('Set workers to %d, workers_init to %d from parameters %s/%s and CPU count %d', $_instances[$api]->workers, $_instances[$api]->workers_init, getenv('bm_param_workers'), getenv('bm_param_workers_init'), getenv('bm_cpu_count')), 'ObjectStorageController::getInstance', __LINE__);
              
              self::log(sprintf('ObjectStorageController implementation %s for API %s instantiated successfully. Initiating...', $controller_class, $api), 'ObjectStorageController::getInstance', __LINE__);
              if (!$_instances[$api]->init()) {
                self::log(sprintf('Unable to initiate API - aborting test'), 'ObjectStorageController::getInstance', __LINE__, TRUE);
                $_instances[$api] = NULL;                
              }
              else if ($_instances[$api]->validate()) {
                self::log(sprintf('Runtime validation successful for API %s', $api), 'ObjectStorageController::getInstance', __LINE__);
                // build test queue
                if ($_instances[$api]->type == 'pull' || $_instances[$api]->type == 'both') {
                  foreach($_instances[$api]->sizes as $size => $bytes) {
                    $_instances[$api]->test_queue[] = array('size' => $size, 'bytes' => $bytes, 'type' => 'pull');
                    self::log(sprintf('Adding test size: %s; bytes: %d; type: PULL to test queue', $size, $bytes), 'ObjectStorageController::getInstance', __LINE__);
                  }
                }
                if ($_instances[$api]->type == 'push' || $_instances[$api]->type == 'both') {
                  foreach($_instances[$api]->sizes as $size => $bytes) {
                    $_instances[$api]->test_queue[] = array('size' => $size, 'bytes' => $bytes, 'type' => 'push');
                    self::log(sprintf('Adding test size: %s; bytes: %d; type: PUSH to test queue', $size, $bytes), 'ObjectStorageController::getInstance', __LINE__);
                  }
                }
              }
              else {
                self::log(sprintf('Runtime parameters are invalid - aborting test'), 'ObjectStorageController::getInstance', __LINE__, TRUE);
                $_instances[$api] = NULL;
              }
            }
            else self::log(sprintf('ObjectStorageController implementation %s for API %s does not extend the base class ObjectStorageController', $controller_class, $api), 'ObjectStorageController::getInstance', __LINE__, TRUE);
          }
        }
        else self::log(sprintf('ObjectStorageController implementation not found for API %s', $api), 'ObjectStorageController::getInstance', __LINE__, TRUE);
      }
      else if ($api) self::log(sprintf('api parameter "%s" is not valid', $api), 'ObjectStorageController::getInstance', __LINE__, TRUE); 
      else self::log('api parameter is not set', 'ObjectStorageController::getInstance', __LINE__, TRUE); 
    }
    
    if (isset($_instances[$api])) return $_instances[$api];
    else return $nl = NULL;
  }
  
  /**
   * Initializes the object storage container. To do so - it first checks if it
   * exists, then creates it if it does not. Returns TRUE on success, FALSE on
   * failure
   * @return boolean
   */
  public final function initContainer() {
    $success = FALSE;
    if (!$this->containerExists($this->container)) {
      self::log(sprintf('Container %s does not exist. Attempting to create...', $this->container), 'ObjectStorageController::initContainer', __LINE__);
      $start = microtime(TRUE);
      if ($success = $this->createContainer($this->container, $this->storage_class)) {
        if ($this->container_wait) sleep($this->container_wait);
        self::log(sprintf('Container %s created successfully (container_wait sleep: %d). %s', $this->container, $this->container_wait, $this->cleanup ? 'Container will be deleted when testing is complete' : ''), 'ObjectStorageController::initContainer', __LINE__);
        exec(sprintf('touch %s/.cleanup-container', getenv('bm_run_dir')));
      }
      else self::log(sprintf('Unable to create container %s', $this->container), 'ObjectStorageController::initContainer', __LINE__, TRUE);
      $this->stat_time_admin += microtime(TRUE) - $start;
    }
    else $success = TRUE;
    return $success;
  }
  
  /**
   * Initializes test objects by creating them where needed within the 
   * designated storage container. Applies to pull/download tests only. No 
   * initialization is necessary to upload/push tests. Returns TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  public final function initObjects() {
    $success = TRUE;
    if ($this->type == 'pull' || $this->type == 'both') {
      foreach($this->sizes as $size => $bytes) {
        $name = str_replace('{resourceId}', getenv('bm_resource_id'), str_replace('{size}', str_replace(' ', '', strtolower($size)), $this->name));
        self::log(sprintf('Checking if test object %s/%s exists and is the correct size...', $this->container, $name), 'ObjectStorageController::initObjects', __LINE__);
        if ($objExists = $this->objectExists($this->container, $name)) $objSize = $this->getSizeCached($name);
        // error
        if ($objExists === NULL || ($objExists && $objSize === NULL)) {
          $success = FALSE;
          self::log(sprintf('Unable to check if object %s/%s exists or get its size', $this->container, $name), 'ObjectStorageController::initObjects', __LINE__, TRUE);
          break;
        }
        else {
          if ($objExists && $objSize != $bytes) {
            $objExists = FALSE;
            self::log(sprintf('Object %s/%s exists but is the incorrect size. Expecting %d bytes; Actual %d bytes. Attempting to delete...', $this->container, $name, $bytes, $objSize), 'ObjectStorageController::initObjects', __LINE__);
            if ($this->deleteObject($this->container, $object)) self::log(sprintf('Object %s/%s deleted successfully', $this->container, $name), 'ObjectStorageController::initObjects', __LINE__);
            else {
              $success = FALSE;
              self::log(sprintf('Unable to delete object %s/%s', $this->container, $name), 'ObjectStorageController::initObjects', __LINE__, TRUE);
              break;
            }
          }
          if (!$objExists) {
            self::log(sprintf('Attempting to create object %s/%s using size %s (%d bytes)', $this->container, $name, $size, $bytes), 'ObjectStorageController::initObjects', __LINE__);
            if ($this->upload($name, $bytes, FALSE, FALSE)) {
              $this->objs[$size] = $name;
              self::log(sprintf('Created object %s/%s successfully', $this->container, $name), 'ObjectStorageController::initObjects', __LINE__);
            }
            else {
              $success = FALSE;
              self::log(sprintf('Unable to initiate upload for object %s/%s using size %s (%d bytes)', $this->container, $name, $size, $bytes), 'ObjectStorageController::initObjects', __LINE__, TRUE);
              break; 
            }
          }
          else {
            $this->objs[$size] = $name;
            self::log(sprintf('Object %s/%s already exists and is the correct size %s (%d bytes)', $this->container, $name, $size, $bytes), 'ObjectStorageController::initObjects', __LINE__);
          }
        }
      }
    }
    return $success;
  }

  /**
   * prints a log message - may be used by implementations to log informational
   * and error messages. Informational messages (when $error=FALSE) are only 
   * logged when the debug runtime parameter is set. Error messages are always 
   * logged
   * @param string $msg the message to output (REQUIRED)
   * @param string $source the source of the message
   * @param int $line an optional line number
   * @param boolean $error is this an error message
   * @param string $source1 secondary source
   * @param int $line1 secondary line number
   * @return void
   */
  public static final function log($msg, $source=NULL, $line=NULL, $error=FALSE, $source1=NULL, $line1=NULL) {
    global $bm_param_debug;
    if (!isset($bm_param_debug)) $bm_param_debug = getenv('bm_param_debug') == '1';
    if ($msg && ($bm_param_debug || $error)) {
      // remove passwords and secrets
      $msg = preg_replace('/Key:\s+([^"]+)/', 'Key: xxx', $msg);
      $msg = preg_replace('/Token:\s+([^"]+)/', 'Token: xxx', $msg);
      $msg = preg_replace('/Authorization:\s+([^"]+)/', 'Authorization: xxx', $msg);
      foreach(array(getenv('bm_param_api_key'), getenv('bm_param_api_secret')) as $secret) {
        if ($secret) {
          $msg = str_replace($secret, 'xxx', $msg);
          $msg = str_replace(urlencode($secret), 'xxx', $msg);
        }
      }
      
    	global $base_error_level;
    	$source = basename($source);
    	if ($source1) $source1 = basename($source1);
    	$exec_time = self::runtime();
    	// avoid timezone errors
    	error_reporting(E_ALL ^ E_NOTICE ^ E_WARNING);
    	$timestamp = date('m/d/Y H:i:s T');
    	error_reporting($base_error_level);
    	printf("%-24s %-12s %-12s %s\n", $timestamp, $exec_time . 's', 
    				 $source ? str_replace('.php', '', $source) . ($line ? ':' . $line : '') : '', 
    				 ($error ? 'ERROR - ' : '') . $msg . 
    				 ($source1 ? ' [' . str_replace('.php', '', $source1) . ($line1 ? ":$line1" : '') . ']' : '')); 
    }
  }
  
  /**
   * returns the arithmetic mean value from an array of points
   * @param array $points an array of numeric data points
   * @return float
   */
	public final function mean($points) {
		$stat = array_sum($points)/count($points);
		$stat = round($stat, self::DEFAULT_ROUND_PRECISION);
		return $stat;
	}

  /**
   * returns the median value from an array of points
   * @param array $points an array of numeric data points
   * @return float
   */
  public final function median($points) {
		sort($points);
		$nmedians = count($points);
		$nmedians2 = floor($nmedians/2);
    $stat = $nmedians % 2 ? $points[$nmedians2] : ($points[$nmedians2 - 1] + $points[$nmedians2])/2;
		$stat = round($stat, self::DEFAULT_ROUND_PRECISION);
		return $stat;
  }
  
  /**
   * performs a test operation - returns TRUE on success, FALSE otherwise
   * @param boolean $record whether or not to return stats for this operation
   * @return boolean
   */
  public final function op($record=TRUE) {
    $success = FALSE;
    if (self::runtime() < $this->duration) {
      $idx = $this->test_queue_pos++;
      if ($this->test_queue_pos >= count($this->test_queue)) $this->test_queue_pos = 0;
      if ($this->randomize) $idx = rand(0, count($this->test_queue) - 1);
      $size = $this->test_queue[$idx]['size'];
      $bytes = $this->test_queue[$idx]['bytes'];
      $type = $this->test_queue[$idx]['type'];
      $obj = $type == 'pull' ? $this->objs[$size] : sprintf('uploadtest%d.bin', rand());
      self::log(sprintf('Initiating op using test index %d; size: %s; bytes: %d; type: %s; object: %s', $idx, $size, $bytes, $type, $obj), 'ObjectStorageController::op', __LINE__);
      $success = ($type == 'pull' ? $this->download($obj, $record) : $this->upload($obj, $bytes, $record)) !== NULL;
    }
    else self::log(sprintf('Current runtime %f exceeds test duration %f - no op will be performed', self::runtime(), $this->duration), 'ObjectStorageController::op', __LINE__);
    return $success;
  }
  
  /**
   * performs rampup operations during the rampup period. returns TRUE if the 
   * test iteration is still in the rampup window following completion of the
   * operation, FALSE otherwise
   * @return boolean
   */
  public final function rampup() {
    $success = FALSE;
    if ($this->rampup && self::runtime() < $this->rampup) {
      $start = microtime(TRUE);
      $success = $this->op(FALSE);
      $this->stat_time_rampup += microtime(TRUE) - $start;
    }
    return $success;
  }
  
  /**
   * returns the current execution time in seconds
   * @return float
   */
  public static final function runtime() {
  	global $start_time;
    if (!isset($start_time)) $start_time = microtime(TRUE);
  	return round(microtime(TRUE) - $start_time, self::DEFAULT_ROUND_PRECISION);
  }
  
  /**
   * converts a size label like 5MB or 1000KB to its numeric byte equivalent. 
   * returns NULL if $size is not valid
   * @param string $size the size label to convert
   * @return int
   */
  public static final function sizeToBytes($size) {
    $bytes = NULL;
    if (is_numeric($size)) $bytes = $size*1;
    else if (preg_match('/^([0-9]+)\s*([gmk]?[b])$/i', trim(strtolower($size)), $m)) {
      $factor = 1;
      switch($m[2]) {
        case 'kb':
          $factor = 1024;
          break;
        case 'mb':
          $factor = 1024*1024;
          break;
        case 'gb':
          $factor = 1024*1024*1024;
          break;
      }
      $bytes = $m[1]*$factor;
    }
    return $bytes;
  }
  
  /**
   * applies operational spacing based on the spacing parameter specified by 
   * the user
   * @return void
   */
  public final function spacing() {
    if ($this->spacing_min > 0) {
      global $bm_param_debug;
      $opTime = (count($this->stat_ops_times) ? $this->stat_ops_times[count($this->stat_ops_times) - 1] : NULL) * 1000000;
  		$min = $this->spacing_min_relative ? $opTime * $this->spacing_min * .01 : $this->spacing_min;
  		$max = isset($this->spacing_max) ? ($this->spacing_max_relative ? $opTime * $this->spacing_max * .01 : $this->spacing_max) : NULL;
  		// if max is greater than the min time, set max to min
  		if (isset($max) && $max < $min) $max = $min;

  		$wait = isset($max) && $min != $max ? rand(round($min), round($max)) : round($min);
  		if ($wait > 0) {
  			$bwait = $wait;
  			if ($this->spacing_adjust_factor) {
  				$nwait = round($wait/$bm_spacing_adjust_factor);
  				self::log(sprintf('Applying %fx sleep reduction due to usleep precision issue. Original sleep was %fµs. New sleep is %fµs', $this->spacing_adjust_factor, $wait, $nwait), 'ObjectStorageController::spacing', __LINE__);
  				$wait = $nwait;
  			}
  			if ($bm_param_debug) {
    			$swait = round($wait/1000000, self::DEFAULT_ROUND_PRECISION);
    			$sopTime = round($opTime/1000000, self::DEFAULT_ROUND_PRECISION);
    			self::log(sprintf('Sleeping %fs (%fµs) for previous op %fs (%fµs) [spacing=%s; min=%f; max=%f; spacing_min=%f; min rel=%d; spacing_max=%f; max rel=%d]', $swait, $wait, $sopTime, $opTime, $this->spacing, $min, $max, $this->spacing_min, $this->spacing_min_relative, $this->spacing_max, $this->spacing_max_relative), 'ObjectStorageController::spacing', __LINE__);
  			}
  			$start = microtime(TRUE);
  			usleep($wait);
  			$this->stat_time_spacing += microtime(TRUE) - $start;
  			$await = round((microtime(TRUE) - $start)*1000000);
  			if (($await - $bwait) > $bwait) {
  				$this->spacing_adjust_factor = round($await/$bwait, self::DEFAULT_ROUND_PRECISION);
  				self::log(sprintf('NOTICE: Actual sleep time %fµs was more than %fx greater than desired %fµs due to inaccuracy of PHP usleep - will attempt offset adjustment for next sleep iteration', $await, $this->spacing_adjust_factor, $bwait), 'ObjectStorageController::spacing', __LINE__);
  			}
  		}
    }
  }
  
  /**
   * prints the current testing stats - see README for a description of output
   * @return void
   */
  function stats() {
    // stat_bw                 => mean aggregate bandwidth (bytes/sec)
    // stat_bw_median          => median aggregate bandwidth (bytes/sec)
    // stat_bw_mbs             => mean aggregate bandwidth (mb/sec)
    // stat_bw_mbs_median      => median aggregate bandwidth (mb/sec)
    // stat_bw_rstdev          => relative standard deviation (sample) of aggregate bandwidth metrics
    // stat_bw_rstdevp         => relative  standard deviation (population) of aggregate bandwidth metrics
    // stat_bw_stdev           => standard deviation (sample) of aggregate bandwidth metrics
    // stat_bw_stdevp          => standard deviation (population) of aggregate bandwidth metrics
    // stat_bw_vals            => used to compute the preceeding 8 metrics
    // stat_ops                => total number of test operations (segmented op counted as 1)
    // stat_ops_failed         => failed test operations
    // stat_ops_failed_ratio   => ratio of failed to total operations as a percentage
    // stat_ops_pull           => pull test operations
    // stat_ops_push           => push test operations
    // stat_ops_secure         => number of secure/https test operations
    // stat_ops_size           => mean size (bytes) of test operations
    // stat_ops_size_median    => median size (bytes) of test operations
    // stat_ops_size_mb        => mean size (megabytes) of test operations
    // stat_ops_size_mb_median => median size (megabytes) of test operations
    // stat_ops_size_vals      => used to compute the preceeding 2 metrics
    // stat_ops_success        => successful test operations
    // stat_ops_success_ratio  => ratio of success to total operations as a percentage
    // stat_ops_times          => operation times - used to compute relative spacing
    // stat_requests           => total number of http requests
    // stat_requests_failed    => failed http requests
    // stat_requests_failed_ratio => ratio of failed to total requests as a percentage
    // stat_requests_pull      => pull test requests
    // stat_requests_push      => push test requests
    // stat_requests_secure    => number of secure/https test requests
    // stat_requests_success   => successful test requests
    // stat_requests_success_ratio => ratio of success to total requests as a percentage
    // stat_segment            => mean segment size (bytes) - if segmented ops performed
    // stat_segment_median     => median segment size (bytes) - if segmented ops performed
    // stat_segment_mb         => mean segment size (megabytes) - if segmented ops performed
    // stat_segment_mb_median  => median segment size (megabytes) - if segmented ops performed
    // stat_segment_vals       => used to compute the preceeding 2 metrics
    // stat_speed              => mean worker bandwidth speed (bytes/sec)
    // stat_speed_median       => median worker bandwidth speed (bytes/sec)
    // stat_speed_mbs          => mean worker bandwidth speed (mb/sec)
    // stat_speed_mbs_median   => median worker bandwidth speed (mb/sec)
    // stat_speed_rstdev       => relative standard deviation (sample) of worker bandwidth metrics
    // stat_speed_rstdevp      => relative standard deviation (population) of worker bandwidth metrics
    // stat_speed_stdev        => standard deviation (sample) of worker bandwidth metrics
    // stat_speed_stdevp       => standard deviation (population) of worker bandwidth metrics
    // stat_speed_vals         => used to compute the preceeding 8 metrics
    // stat_status_codes       => http status codes returned by the storage service and their frequency (e.g. 200/10; 404/2)
    // stat_time               => total test time including admin, rampup and spacing (secs)
    // stat_time_admin         => duration of administrative test operations (secs)
    // stat_time_ops           => duration of test operations included in the stats (secs)
    // stat_time_rampup        => duration of rampup test operations (secs)
    // stat_time_spacing       => duration of spacing between test operations (secs)
    // stat_transfer           => total bytes transferred (excluding rampup)
    // stat_transfer_mb        => total megabytes transferred (excluding rampup)
    // stat_transfer_pull      => bytes transferred for pull operations
    // stat_transfer_pull_mb   => megabytes transferred for pull operations
    // stat_transfer_push      => bytes transferred for push operations
    // stat_transfer_push_mb   => megabytes transferred for push operations
    // stat_workers            => mean concurrent workers
    // stat_workers_median     => median concurrent workers
    // stat_workers_per_cpu    => mean concurrent workers per CPU cores (workers/[# CPU cores])
    // stat_workers_vals       => used to compute the preceeding 3 metrics
    printf("bw=%s\n", $bw = $this->mean($this->stat_bw_vals));
    printf("bw_median=%s\n", $bw_median = $this->median($this->stat_bw_vals));
    printf("bw_mbs=%s\n", round((($bw*8)/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("bw_mbs_median=%s\n", round((($bw_median*8)/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("bw_rstdev=%s\n", $this->stdev($this->stat_bw_vals, 3));
    printf("bw_rstdevp=%s\n", $this->stdev($this->stat_bw_vals, 4));
    printf("bw_stdev=%s\n", $this->stdev($this->stat_bw_vals));
    printf("bw_stdevp=%s\n", $this->stdev($this->stat_bw_vals, 2));
    printf("ops=%d\n", $this->stat_ops);
    printf("ops_failed=%d\n", $this->stat_ops_failed);
    printf("ops_failed_ratio=%s\n", $this->stat_ops ? round(($this->stat_ops_failed/$this->stat_ops)*100, self::DEFAULT_ROUND_PRECISION) : '0');
    printf("ops_pull=%d\n", $this->stat_ops_pull);
    printf("ops_push=%d\n", $this->stat_ops_push);
    printf("ops_secure=%d\n", $this->stat_ops_secure);
    printf("ops_size=%d\n", round($this->mean($this->stat_ops_size_vals)));
    printf("ops_size_median=%d\n", round($this->median($this->stat_ops_size_vals)));
    printf("ops_size_mb=%s\n", round(($this->mean($this->stat_ops_size_vals)/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("ops_size_mb_median=%s\n", round(($this->median($this->stat_ops_size_vals)/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("ops_success=%d\n", $this->stat_ops_success);
    printf("ops_success_ratio=%s\n", $this->stat_ops ? round(($this->stat_ops_success/$this->stat_ops)*100, self::DEFAULT_ROUND_PRECISION) : '0');
    printf("requests=%d\n", $this->stat_requests);
    printf("requests_failed=%d\n", $this->stat_requests_failed);
    printf("requests_failed_ratio=%s\n", $this->stat_requests ? round(($this->stat_requests_failed/$this->stat_requests)*100, self::DEFAULT_ROUND_PRECISION) : '0');
    printf("requests_pull=%d\n", $this->stat_requests_pull);
    printf("requests_push=%d\n", $this->stat_requests_push);
    printf("requests_secure=%d\n", $this->stat_requests_secure);
    printf("requests_success=%d\n", $this->stat_requests_success);
    printf("requests_success_ratio=%s\n", $this->stat_requests ? round(($this->stat_requests_success/$this->stat_requests)*100, self::DEFAULT_ROUND_PRECISION) : '0');
    printf("segment=%d\n", round($this->mean($this->stat_segment_vals)));
    printf("segment_median=%d\n", round($this->median($this->stat_segment_vals)));
    printf("segment_mb=%s\n", round(($this->mean($this->stat_segment_vals)/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("segment_mb_median=%s\n", round(($this->median($this->stat_segment_vals)/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("speed=%s\n", $speed = $this->mean($this->stat_speed_vals));
    printf("speed_median=%s\n", $speed_median = $this->median($this->stat_speed_vals));
    printf("speed_mbs=%s\n", round((($speed*8)/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("speed_mbs_median=%s\n", round((($speed_median*8)/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("speed_rstdev=%s\n", $this->stdev($this->stat_speed_vals, 3));
    printf("speed_rstdevp=%s\n", $this->stdev($this->stat_speed_vals, 4));
    printf("speed_stdev=%s\n", $this->stdev($this->stat_speed_vals));
    printf("speed_stdevp=%s\n", $this->stdev($this->stat_speed_vals, 2));
    $status_codes = '';
    ksort($this->stat_status_codes);
    foreach($this->stat_status_codes as $status => $count) $status_codes .= ($status_codes ? '; ' : '') . $status . '/' . $count;
    printf("status_codes=%s\n", $status_codes);
    printf("time=%s\n", $this->stat_time_admin + $this->stat_time_ops + $this->stat_time_rampup + $this->stat_time_spacing);
    printf("time_admin=%s\n", $this->stat_time_admin);
    printf("time_ops=%s\n", $this->stat_time_ops);
    printf("time_rampup=%s\n", $this->stat_time_rampup);
    printf("time_spacing=%s\n", $this->stat_time_spacing);
    printf("transfer=%d\n", $this->stat_transfer);
    printf("transfer_mb=%s\n", round(($this->stat_transfer/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("transfer_pull=%d\n", $this->stat_transfer_pull);
    printf("transfer_pull_mb=%s\n", round(($this->stat_transfer_pull/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("transfer_push=%d\n", $this->stat_transfer_push);
    printf("transfer_push_mb=%s\n", round(($this->stat_transfer_push/1024)/1024, self::DEFAULT_ROUND_PRECISION));
    printf("workers=%d\n", round($this->mean($this->stat_workers_vals)));
    printf("workers_median=%d\n", round($this->median($this->stat_workers_vals)));
    printf("workers_per_cpu=%s\n", round($this->mean($this->stat_workers_vals)/getenv('bm_cpu_count'), self::DEFAULT_ROUND_PRECISION));
  }
  
  /**
   * computes a standard deviation for the $points specified
   * @param array $points an array of numeric data points
	 * @param int $type the type of standard deviation metric to return. One of 
	 * the following numeric identifiers:
	 *   1 = sample standard deviation (DEFAULT)
	 *   2 = population standard deviation
	 *   3 = relative sample standard deviation
	 *   4 = relative population standard deviation
	 *   5 = sample variance
	 *   6 = population variance
   * @return float
   */
  function stdev($points, $type=1) {
    $mean = array_sum($points)/count($points);
    $variance = 0.0;
    foreach ($points as $i) $variance += pow($i - $mean, 2);
    $variance /= ($type == 1 || $type == 3 || $type == 5 ? count($points) - 1 : count($points));
		if ($type == 5 || $type == 6) return $variance;
    $stddev = (float) sqrt($variance);
		if ($type > 2) $stddev = 100 * ($stddev/$mean);
		$stddev = round($stddev, self::DEFAULT_ROUND_PRECISION);
		return $stddev;
  }
  
  /**
   * uploads an object containing $bytes of random data to the designated 
   * test container using $name for the object name. Returns one of the 
    * following values:
    *   TRUE:  successful
    *   FALSE: failed due to http error
    *   NULL:  curl failure (non service related)
   * @param string $name the name for the object to upload
   * @param int $bytes the desired object size - objects are filled with 
   * random data using /dev/urandom (using the urandom.php script)
   * @param boolean $record whether or not to record stats for this operation
   * @param boolean $testObject whether or not this is for a test operation 
   * (as opposed to uploading an object for the purpose of download testing)
   * @return boolean
   */
  public final function upload($name, $bytes, $record=TRUE, $testObject=TRUE) {
    $success = NULL;
    $parts = NULL;
    $partSize = NULL;
    $lastPartSize = NULL;
    // determine number of parts to upload concurrently
    $workers = $testObject ? $this->workers : $this->workers_init;
    if ($workers > 1 && $this->segmentBytes) {
      $parts = ceil($bytes/$this->segmentBytes);
      if ($parts > $workers) $parts = $workers;
      if ($parts > 1) {
        $partSize = round($bytes/$parts);
        $lastPartSize = $bytes - ($partSize * ($parts - 1));
      }
      else $parts = NULL;
    }
    self::log(sprintf('Initiating upload of %d bytes to %s/%s using %d parts. Stats will%s be recorded', $bytes, $this->container, $name, $parts ? $parts : 1, $record ? '' : ' not'), 'ObjectStorageController::upload', __LINE__);
    
    // upload size exceeds max allowed => convert to multipart
    if ($this->multipartSupported() && $this->uploadMaxSize() && !$parts && $bytes > $this->uploadMaxSize()) {
      $parts = ceil($bytes/($this->multipartMaxSegment() ? $this->multipartMaxSegment() : $this->uploadMaxSize()));
      $partSize = round($bytes/$parts);
      $lastPartSize = $bytes - ($partSize * ($parts - 1));
      self::log(sprintf('Changed single worker upload to %d parts of %d bytes because %d bytes is more than the max allowed %d by the storage platform', $parts, $partSize, $bytes, $this->uploadMaxSize()), 'ObjectStorageController::upload', __LINE__);
    }
    // multipart segment size exceeds max allowed - use more segments
    if ($this->multipartSupported() && $this->multipartMaxSegment() && $parts && $partSize > $this->multipartMaxSegment()) {
      $oparts = $parts;
      $opartSize = $partSize;
      $parts = ceil($bytes/$this->multipartMaxSegment());
      $partSize = round($bytes/$parts);
      $lastPartSize = $bytes - ($partSize * ($parts - 1));
      self::log(sprintf('Changed number of parts from %d to %d of %d bytes because the original part size %d exceeded the max allowed %d', $oparts, $parts, $partSize, $opartSize, $this->multipartMaxSegment()), 'ObjectStorageController::upload', __LINE__);
    }
    
    $uploads = array();
    $numRequests = $parts ? $parts : ($testObject && !$this->segmentBytes ? $this->workers : 1);
    // simulate multipart uploads
    if ($record) $start = microtime(TRUE);
    if ($parts > 1 && !$this->multipartSupported()) {
      for($i=1; $i<=$numRequests; $i++) {
        $size = $i == $numRequests ? $lastPartSize : $partSize;
        $uploads[$i] = $this->initUpload($this->container, $name . '.' . $i, $size, $this->encryption, $this->storage_class);
      }
    }
    else $uploads[1] = $this->initUpload($this->container, $name, $bytes, $this->encryption, $this->storage_class, $parts);
    if ($record) $this->stat_time_admin += microtime(TRUE) - $start;
    
    if ($uploads && $uploads[1] && isset($uploads[1]['url']) && preg_match('/^http/i', $uploads[1]['url']) && 
        isset($uploads[1]['headers']) && is_array($uploads[1]['headers']) && count($uploads[1]['headers'])) {
      $requests = array();
      // build headers for the curl requests
      for($i=1; $i<=$numRequests; $i++) {
        $size = $parts ? ($i == $numRequests ? $lastPartSize : $partSize) : $bytes;
        $upload = isset($uploads[$i]) ? $uploads[$i] : $uploads[1];
        $request = array('url' => $upload['url'], 'method' => isset($upload['method']) ? $upload['method'] : 'PUT', 'headers' => isset($upload['headers'][$i - 1]) ? $upload['headers'][$i - 1] : $upload['headers']);
        // remove content-length header - this is set dynamically
        foreach(array_keys($request['headers']) as $key) if (strtolower($key) == 'content-length' || strtolower($key) == 'content-type') unset($request['headers'][$key]);
        $request['headers']['content-length'] = $size;
        $request['headers']['content-type'] = self::CONTENT_TYPE;
        $request['url'] = str_replace('{size}', $size, $request['url']);
        if ($parts) {
          $request['url'] = str_replace('{part}', $i, $request['url']);
          $request['url'] = str_replace('{part_base64}', base64_encode(sprintf('%04d', $i)), $request['url']);
        }
        foreach(array_keys($request['headers']) as $key) {
          $request['headers'][$key] = str_replace('{size}', $size, $request['headers'][$key]);
          if ($parts) {
            $request['headers'][$key] = str_replace('{part}', $i, $request['headers'][$key]);
            $request['headers'][$key] = str_replace('{part_base64}', base64_encode(sprintf('%04d', $i)), $request['headers'][$key]);
          }
        }
        $requests[] = $request;
        self::log(sprintf('Added %d byte upload request %d with URL %s', $size, $i, $request['url']), 'ObjectStorageController::upload', __LINE__);
      }
      $curl = array();
      foreach($requests as $request) {
        $input = sprintf('%s/urandom.php %d', getenv('bm_run_dir'), $request['headers']['content-length']);
        $curl[] = array('method' => 'PUT', 'headers' => $request['headers'], 'url' => $request['url'], 'input' => $input);
        self::log(sprintf('Added curl request for %s', $request['url']), 'ObjectStorageController::upload', __LINE__);
      }
      
      if (count($requests) > $workers) {
        self::log(sprintf('Processing requests in %d request batches because %d exceeds the number of allowed workers %d', $workers, count($requests), $workers), 'ObjectStorageController::upload', __LINE__);
        $result = array('urls' => array(), 'request' => array(), 'response' => array(), 'results' => array(), 'status' => array());
        $pos = 0;
        for($i=0; $i<count($curl); $i += $workers) {
          self::log(sprintf('Processing request batch %d-%d of %d', $i+1, ($i+$workers)<count($curl) ? $i+$workers : count($curl), count($curl)), 'ObjectStorageController::upload', __LINE__);
          if ($r = $this->curl(array_slice($curl, $i, $workers), FALSE, $record)) {
            self::log(sprintf('Request batch %d-%d of %d completed successfully', $i+1, $i+1+$workers, count($curl)), 'ObjectStorageController::upload', __LINE__);
            if (!isset($result['lowest_status']) || $result['lowest_status'] > $r['lowest_status']) $result['lowest_status'] = $r['lowest_status'];
            if (!isset($result['highest_status']) || $result['highest_status'] < $r['highest_status']) $result['highest_status'] = $r['highest_status'];
            foreach(array('urls', 'request', 'response', 'results', 'status') as $key) {
              foreach($r[$key] as $val) $result[$key][] = $val;
            }
          }
          else {
            self::log(sprintf('Unable to invoke batched requests for %d workers at offset %d', $workers, $i), 'ObjectStorageController::upload', __LINE__, TRUE);
            $result = NULL;
            break;
          }
        }
        self::log(sprintf('Worker batch processing complete'), 'ObjectStorageController::upload', __LINE__);
      }
      else $result = $this->curl($curl, FALSE, $record);
      
      if ($result) {
        if ($record) $start = microtime(TRUE);
        if ($result['highest_status'] < 400 && ($parts <= 1 || !$this->multipartSupported() || $this->completeMultipartUpload($this->container, $name, $result))) {
          if ($record) $this->stat_time_admin += microtime(TRUE) - $start;
          $success = TRUE;
          foreach(array_keys($uploads) as $i) {
            $upload = isset($uploads[$i]) ? $uploads[$i] : $uploads[1];
            $pieces = explode('?', basename($upload['url']));
            exec(sprintf('echo "%s" >> %s/%s', $pieces[0], getenv('bm_run_dir'), $testObject ? '.test-objects' : '.cleanup-objects'));
          }
          self::log(sprintf('Successfully uploaded object %s/%s in %d parts', $this->container, $name, $parts ? $parts : 1), 'ObjectStorageController::upload', __LINE__);
        }
        else {
          $success = $result['lowest_status'] && ($result['lowest_status'] < 400 || in_array($result['lowest_status'], $this->continue_errors)) ? FALSE : NULL;
          self::log(sprintf('Failed to upload object %s/%s in %d parts. lowest status %d; highest status %d', $this->container, $name, $parts, $result['lowest_status'], $result['highest_status']), 'ObjectStorageController::upload', __LINE__, TRUE);
        }
      }
      else self::log(sprintf('One or more curl processes failed during upload'), 'ObjectStorageController::upload', __LINE__, TRUE);
    }
    else self::log(sprintf('Unable to initiate upload for %s/%s', $this->container, $name), 'ObjectStorageController::upload', __LINE__, TRUE);
    return $success;
  }
  
  /**
   * Validates runtime parameters - returns TRUE on success, FALSE on failure.
   * If a validation failure occurs, the relevant error message will be logged
   */
  public final function validate() {
    if (!isset($this->validated)) {
      $this->validated = TRUE;
      
      // validate object sizes
      if (!count($this->sizes)) {
        self::log(sprintf('No valid size values specified in %s', $this->size), 'ObjectStorageController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      else {
        foreach($this->sizes as $size => $bytes) {
          if ($bytes > self::MAX_OBJECT_SIZE) {
            self::log(sprintf('Object size %s exceeds maximum allowed size %d GB', $size, self::MAX_OBJECT_SIZE/1024/1024/1024), 'ObjectStorageController::validate', __LINE__, TRUE);
            $this->validated = FALSE;
          }
        }
      }
      
      // validate object naming convention
      if ($this->validated && count($this->sizes) > 1 && strpos($this->name, '{size}') === FALSE) {
        self::log(sprintf('Object name %s must contain the token {size} in order to support multiple object sizes during testing', $this->name), 'ObjectStorageController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // validate segment size
      if ($this->validated && $this->segmentBytes && $this->segmentBytes < $this->multipartMinSegment()) {
        self::log(sprintf('Segment size %s cannot be less than %d bytes', $this->segment, $this->multipartMinSegment()), 'ObjectStorageController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      
      // validate upload size
      if ($this->validated && ($this->type == 'push' || $this->type == 'both')) {
        foreach ($this->sizes as $label => $size) {
          if ($this->uploadMaxSize() && $size > $this->uploadMaxSize() && !$this->segmentBytes) {
            self::log(sprintf('Upload size %s cannot be greater than %d bytes', $label, $this->uploadMaxSize()), 'ObjectStorageController::validate', __LINE__, TRUE);
            $this->validated = FALSE;
          }
          else if ($this->segmentBytes && $this->multipartMaxSegment() && $this->segmentBytes > $this->multipartMaxSegment()) {
            self::log(sprintf('Segment size %s cannot be greater than %d bytes', $this->segment, $this->multipartMaxSegment()), 'ObjectStorageController::validate', __LINE__, TRUE);
            $this->validated = FALSE;
          }
        } 
      }
      
      $start = microtime(TRUE);
      if (!$this->authenticate()) {
        self::log(sprintf('Authentication failed using key %s; region %s; endpoint %s', $this->api_key, $this->api_region, $this->api_endpoint), 'ObjectStorageController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      else self::log(sprintf('Authentication successful using key %s; region %s; endpoint %s', $this->api_key, $this->api_region, $this->api_endpoint), 'ObjectStorageController::validate', __LINE__);
      $this->stat_time_admin += microtime(TRUE) - $start;
      
      // service specific validations
      if (!$this->validateApi($this->api_endpoint, $this->api_region, $this->api_ssl, $this->dns_containers, $this->encryption, $this->storage_class)) {
        self::log('API/service validation failed', 'ObjectStorageController::validate', __LINE__, TRUE);
        $this->validated = FALSE;
      }
      else self::log('API/service validation successful', 'ObjectStorageController::validate', __LINE__);
    }
    return $this->validated;
  }
  
  
  // these methods may by overriden by an API implementation
  
  /**
   * Invoked following completion of a multipart upload. return TRUE if 
   * the multipart upload was successful, FALSE if it failed
   * @param string $container the container uploaded to
   * @param string $object the name of the object uploaded
   * @param array $results return value from invoking the 'curl' method
   * @return boolean
   */
  public function completeMultipartUpload($container, $object, $results) {
    return NULL;
  }
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    return TRUE;
  }
  
  /**
   * may be overridden to define a maximum segment size in bytes for multipart 
   * uploads. If NULL, no maximum size constraint will be applied
   * @return int
   */
  protected function multipartMaxSegment() {
    return NULL;
  }
  
  /**
   * may be overridden to define a minimum segment size in bytes for multipart 
   * uploads (default is 5 MB). If NULL, no minimum size constraint will be 
   * applied
   * @return int
   */
  protected function multipartMinSegment() {
    return self::DEFAULT_MULTIPART_MIN_SEGMENT;
  }
  
  /**
   * may be overridden if multipart uploads are supported
   * @return boolean
   */
  protected function multipartSupported() {
    return FALSE;
  }
  
  /**
   * may be overridden if range requests are not supported
   * @return boolean
   */
  protected function rangeRequestsSupported() {
    return TRUE;
  }
  
  /**
   * may be overridden to define a maximum single request upload size in bytes.
   * If NULL, no maximum size constraint will be applied
   * @return int
   */
  protected function uploadMaxSize() {
    return NULL;
  }
  
  /**
   * may be overridden to perform additional API/service specific validations
   * including validation of the runtime parameters listed below (see README 
   * for details). Return TRUE if validation passes, FALSE otherwise
   * @param string $api_endpoint
   * @param string $api_region
   * @param boolean $api_ssl
   * @param boolean $dns_containers 
   * @param boolean $encryption 
   * @param boolean $storage_class 
   * @return boolean
   */
  protected function validateApi($api_endpoint, $api_region, $api_ssl, $dns_containers, $encryption, $storage_class) {
    return TRUE;
  }
  
  
  // these methods must be defined for each API implementation
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method should reference the instance
   * attributes $api_key, $api_secret, $api_endpoint and $api_region as 
   * necessary to complete the authentication
   * @return boolean
   */
  abstract protected function authenticate();
  
  /**
   * returns TRUE if $container exists, FALSE otherwise. return NULL on 
   * error
   * @param string $container the container to check 
   * @return boolean
   */
  abstract public function containerExists($container);
  
  /**
   * creates $container. returns TRUE on success, FALSE on failure
   * @param string $container the container to create
   * @param string $storage_class optional service specific storage class to 
   * apply to the container
   * @return boolean
   */
  abstract public function createContainer($container, $storage_class=NULL);
  
  /**
   * deletes $container. returns TRUE on success, FALSE on failure
   * @param string $container the container to delete 
   * @return boolean
   */
  abstract public function deleteContainer($container);
  
  /**
   * deletes $object. returns TRUE on success, FALSE on failure
   * @param string $container the object container
   * @param string $object name of the object to delete
   * @return boolean
   */
  abstract public function deleteObject($container, $object);
  
  /**
   * returns the size of $object in bytes. return NULL on error
   * @param string $container the object container
   * @param string $object name of the object
   * @return int
   */
  abstract public function getObjectSize($container, $object);
  
  /**
   * Download initialization method. Returns a hash defining the URL and 
   * headers to use to perform the download. This hash may contain the 
   * following keys:
   *   url          REQUIRED / Complete URL to use for the request. Should be 
   *                https if the $this->api_ssl flag is set and supported by 
   *                the storage platform
   *   method       http method (if other than GET)
   *   headers      REQUIRED / hash containing request headers
   * @param string $container the container to download from
   * @param string $object the name of the object to download
   * @return array
   */
  abstract public function initDownload($container, $object);
  
  /**
   * Upload initialization method. Returns a hash defining the URL and headers 
   * to use to perform the upload. This hash may contain the following keys:
   *   url          REQUIRED / Complete URL to use for the request. Should be 
   *                https if the $this->api_ssl flag is set and supported by 
   *                the storage platform. may contain the following dynamic 
   *                tokens:
   *                  {size}        replaced with the byte size of the file/part
   *                  {part}        replaced with incrementing numeric value 
   *                                corresponding with the part number
   *                  {part_base64} replaced with incrementing base64 encoded 
   *                                numeric value corresponding with the part 
   *                                number
   *   method       http method (if other than PUT)
   *   headers      REQUIRED / hash containing request headers. header values 
   *                may contain the same dynamic tokens as 'url'. The 
   *                Content-Length header will be added automatically. If 
   *                $parts > 1, headers may be an ordered array of hashes, for 
   *                each corresponding part request
   * @param string $container the container to upload to
   * @param string $object the name of the object to upload to
   * @param int $bytes size for the object
   * @param string $encryption optional service specific encryption to apply to
   * the created object
   * @param string $storage_class optional service specific storage class to 
   * apply to the object
   * @param int $parts used for multipart uploads when supported. when set, it
   * is a numeric value > 1 identifying the number of separate parts that will
   * be uploaded
   * @return array
   */
  abstract public function initUpload($container, $object, $bytes, $encryption=NULL, $storage_class=NULL, $parts=NULL);
  
  /**
   * returns TRUE if the object identified by $name exists in $container. 
   * return NULL on error
   * @param string $container the container to check
   * @param string $object the name of the object
   * @return boolean
   */
  abstract public function objectExists($container, $object);
  
}
?>
