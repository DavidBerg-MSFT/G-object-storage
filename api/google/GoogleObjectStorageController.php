<?php
/**
 * ObjectStorageController implementation for the Google Cloud Storage (GCS) 
 * API
 */
class GoogleObjectStorageController extends ObjectStorageController {
  // default API endpoint for GCS
  const DEFAULT_GOOGLE_ENDPOINT = 'storage.googleapis.com';
  const DEFAULT_REGION = 'US';
  const SIGNATURE_DATE_FORMAT = 'D, d M Y H:i:s O';
  const STORAGE_CLASS_STANDARD = 'STANDARD';
  const STORAGE_CLASS_DRA = 'DURABLE_REDUCED_AVAILABILITY';
  const CONTAINERS_GLOBAL = 'US,EU';
  const CONTAINERS_REGIONAL = 'US-EAST1,US-EAST2,US-EAST3,US-CENTRAL1,US-CENTRAL2,US-WEST1';
  
  // api endpoint url
  private $api_url;
  // is api_region a regional container
  private $api_region_regional = FALSE;
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method should reference the instance
   * attributes $api_key, $api_secret, $api_endpoint and $api_region as 
   * necessary to complete the authentication
   * @return boolean
   */
  protected function authenticate() {
    // test authentication by listing containers
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('GET', $headers);
    $request = array('method' => 'GET', 'url' => $this->getUrl(), 'headers' => $headers);
    $success = NULL;
    if ($result = $this->curl(array($request))) {
      $success = $result['status'][0] == 200 || $result['status'][0] == 404;
      self::log(sprintf('GET Service request completed - status %d. Authentication was%s successful', $result['status'][0], $success ? '' : ' not'), 'GoogleObjectStorageController::authenticate', __LINE__);
    }
    else self::log(sprintf('GET Service request failed'), 'GoogleObjectStorageController::authenticate', __LINE__, TRUE);
    return $success;
  }
  
  /**
   * returns TRUE if $container exists, FALSE otherwise. return NULL on 
   * error
   * @param string $container the container to check 
   * @return boolean
   */
  public function containerExists($container) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container), 'headers' => $headers);
    $exists = NULL;
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] == 200;
      self::log(sprintf('HEAD Bucket request completed - status %d. Bucket %s does%s exist', $result['status'][0], $container, $exists ? '' : ' not'), 'GoogleObjectStorageController::containerExists', __LINE__);
    }
    else self::log(sprintf('HEAD Bucket request failed'), 'GoogleObjectStorageController::containerExists', __LINE__, TRUE);
    return $exists;
  }
  
  /**
   * creates $container. returns TRUE on success, FALSE on failure
   * @param string $container the container to create
   * @param string $storage_class optional service specific storage class to 
   * apply to the container - forced to DRA for regional containers (see 
   * README)
   * @return boolean
   */
  public function createContainer($container, $storage_class=NULL) {
    $storage_class = $this->api_region_regional ? self::STORAGE_CLASS_DRA : ($storage_class ? $storage_class : self::STORAGE_CLASS_STANDARD);
    $xml = sprintf('<CreateBucketConfiguration><LocationConstraint>%s</LocationConstraint><StorageClass>%s</StorageClass></CreateBucketConfiguration>', trim(strtoupper($this->api_region)), $storage_class);
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    if ($xml) $headers['content-type'] = 'text/plain';
    $headers['Authorization'] = $this->sign('PUT', $headers, $container);
    $request = array('method' => 'PUT', 'url' => $this->getUrl($container, NULL, NULL, FALSE), 'headers' => $headers);
    if ($xml) $request['body'] = $xml;
    $created = NULL;
    if ($result = $this->curl(array($request))) {
      $created = $result['status'][0] == 200;
      self::log(sprintf('PUT Bucket %s request completed for region %s - status %d. Bucket %s %s. XML: %s', $container, $this->api_region, $result['status'][0], $container, $created ? 'created successfully' : 'could not be created', $xml), 'GoogleObjectStorageController::createContainer', __LINE__, !$created);
    }
    else self::log(sprintf('PUT Bucket %s request failed for region %s using xml: %s', $container, $this->api_region, $xml), 'GoogleObjectStorageController::createContainer', __LINE__, TRUE);
    return $created;
  }
  
  /**
   * deletes $container. returns TRUE on success, FALSE on failure
   * @param string $container the container to delete 
   * @return boolean
   */
  public function deleteContainer($container) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('DELETE', $headers, $container);
    $request = array('method' => 'DELETE', 'url' => $this->getUrl($container), 'headers' => $headers);
    $deleted = NULL;
    if ($result = $this->curl(array($request))) {
      $deleted = $result['status'][0] == 204;
      self::log(sprintf('DELETE Bucket request completed - status %d', $result['status'][0], $container), 'GoogleObjectStorageController::deleteContainer', __LINE__);
    }
    else self::log(sprintf('DELETE Bucket request failed'), 'GoogleObjectStorageController::deleteContainer', __LINE__, TRUE);
    return $deleted;
  }
  
  /**
   * deletes $object. returns TRUE on success, FALSE on failure
   * @param string $container the object container
   * @param string $object name of the object to delete
   * @return boolean
   */
  public function deleteObject($container, $object) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('DELETE', $headers, $container, $object);
    $request = array('method' => 'DELETE', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $deleted = NULL;
    if ($result = $this->curl(array($request))) {
      $deleted = $result['status'][0] == 204;
      self::log(sprintf('DELETE Object request completed - status %d', $result['status'][0], $container), 'GoogleObjectStorageController::deleteObject', __LINE__);
    }
    else self::log(sprintf('DELETE Object request failed'), 'GoogleObjectStorageController::deleteObject', __LINE__, TRUE);
    return $deleted;
  }
  
  /**
   * returns the size of $object in bytes. return NULL on error
   * @param string $container the object container
   * @param string $object name of the object
   * @return int
   */
  public function getObjectSize($container, $object) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container, $object);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $size = NULL;
    if ($result = $this->curl(array($request))) {
      if (($exists = $result['status'][0] == 200) && isset($result['response'][0]['content-length'])) $size = $result['response'][0]['content-length'];
      self::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist. %s', $result['status'][0], $container, $object, $exists ? '' : ' not', $size ? 'Size is ' . $size . ' bytes' : ''), 'GoogleObjectStorageController::getObjectSize', __LINE__);
    }
    else self::log(sprintf('HEAD Object request failed'), 'GoogleObjectStorageController::getObjectSize', __LINE__, TRUE);
    return $size;
  }
  
  /**
   * Returns the GCS API URL to use for the specified $container and $object
   * @param string $container the container to return the URL for
   * @param string $object optional object to include in the URL
   * @param array $params optional URL parameters
   * @param boolean $dnsContainers may be used to override $this->dns_containers
   * @return string
   */
  private function getUrl($container=NULL, $object=NULL, $params=NULL, $dnsContainers=NULL) {
    $url = $this->api_url;
    if ($container) {
      $dns_containers = $dnsContainers !== NULL ? $dnsContainers : $this->dns_containers;
      if ($dns_containers) $url = str_replace('://', '://' . $container . '.', $url);
      $url = sprintf('%s%s%s', $url, $dns_containers ? '' : '/' . $container, $object ? '/' . $object : '');
      if (is_array($params)) {
        foreach(array_keys($params) as $i => $param) {
          $url .= ($i ? '&' : '?') . $param . ($params[$param] ? '=' . $params[$param] : '');
        }
      }
    }
    return $url;
  }
  
  /**
   * may be used to perform pre-usage initialization. return TRUE on success, 
   * FALSE on failure
   * @return boolean
   */
  protected function init() {
    
    if (!$this->api_region) $this->api_region = self::DEFAULT_REGION;
    $this->api_region_regional = in_array(trim(strtoupper($this->api_region)), explode(',', self::CONTAINERS_REGIONAL));
		$this->api_url = preg_match('/^http/i', $this->api_endpoint) ? $this->api_endpoint : ($this->api_ssl ? 'https://' : 'http://') . self::DEFAULT_GOOGLE_ENDPOINT;
    self::log(sprintf('Set GCS API URL to %s', $this->api_url), 'GoogleObjectStorageController::init', __LINE__);
    
    return TRUE;
  }
  
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
  public function initDownload($container, $object) {
    $request = NULL;
    if ($container && $object) {
      $request = array('headers' => array('date' => gmdate(self::SIGNATURE_DATE_FORMAT)));
      $request['url'] = $this->getUrl($container, $object);
      $request['headers']['Authorization'] = $this->sign('GET', $request['headers'], $container, $object);
      self::log(sprintf('Initialized download for %s/%s. Returning URL %s and signature %s', $container, $object, $request['url'], $request['headers']['Authorization']), 'GoogleObjectStorageController::initDownload', __LINE__);
    }
    return $request;
  }
  
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
  public function initUpload($container, $object, $bytes, $encryption=NULL, $storage_class=NULL, $parts=NULL) {
    $request = array('headers' => array('content-type' => self::CONTENT_TYPE, 'date' => gmdate(self::SIGNATURE_DATE_FORMAT)));
    $url = $this->getUrl($container, $object);
    $params = NULL;
    $request['url'] = $url;
    $signature = $this->sign('PUT', $request['headers'], $container, $object, $params);
    $request['headers']['Authorization'] = $signature;
    return $request;
  }
  
  /**
   * returns TRUE if the object identified by $name exists in $container. 
   * return NULL on error
   * @param string $container the container to check
   * @param string $object the name of the object
   * @return boolean
   */
  public function objectExists($container, $object) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container, $object);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $exists = NULL;
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] == 200;
      self::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist', $result['status'][0], $container, $object, $exists ? '' : ' not'), 'GoogleObjectStorageController::objectExists', __LINE__);
    }
    else self::log(sprintf('HEAD Object request failed'), 'GoogleObjectStorageController::objectExists', __LINE__, TRUE);
    return $exists;
  }
  
  /**
   * returns an authorization signature for the parameters specified
   * @param string $method the http method
   * @param array $headers http headers
   * @param string $container optional container
   * @param string $object optional object to create the signature for
   * @return string
   */
  private function sign($method, $headers, $container=NULL, $object=NULL) {
    // add goog headers to signature
    $goog_headers = array();
    foreach($headers as $key => $val) {
      if (preg_match('/^x-goog/', $key)) $goog_headers[strtolower($key)] = $val;
    }
    ksort($goog_headers);
    $goog_string = '';
    foreach($goog_headers as $key => $val) $goog_string .= $key . ':' . trim($val) . "\n";
    
    $uri = '';
    if ($object) $uri = $container . '/' . $object;
    else if ($method == 'PUT' && !$object) $uri = $container;
    else if ($container) $uri = $container . '/';
    $string = sprintf("%s\n\n%s\n%s\n%s/%s", 
                      strtoupper($method),
                      isset($headers['content-type']) ? $headers['content-type'] : '',
                      $headers['date'], 
                      $goog_string,
                      $uri);
    self::log(sprintf('Signing string %s', str_replace("\n", '\n', $string)), 'GoogleObjectStorageController::sign', __LINE__);
		$signature = base64_encode(extension_loaded('hash') ? hash_hmac('sha1', $string, $this->api_secret, TRUE) : pack('H*', sha1((str_pad($this->api_secret, 64, chr(0x00)) ^ (str_repeat(chr(0x5c), 64))) . pack('H*', sha1((str_pad($this->api_secret, 64, chr(0x00)) ^ (str_repeat(chr(0x36), 64))) . $string)))));
		return sprintf('GOOG1 %s:%s', $this->api_key, $signature);
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
    $validated = TRUE;
    $api_region = trim(strtoupper($api_region));
    if (!$api_endpoint && $api_region && !in_array($api_region, explode(',', self::CONTAINERS_GLOBAL)) && !in_array($api_region, explode(',', self::CONTAINERS_REGIONAL))) {
      $validated = FALSE;
      self::log(sprintf('api_region %s is not valid. Valid options are: %s OR %s', $api_region, self::CONTAINERS_GLOBAL, self::CONTAINERS_REGIONAL), 'GoogleObjectStorageController::validateApi', __LINE__, TRUE);
    }
    if ($encryption) {
      $validated = FALSE;
      self::log(sprintf('encryption parameter %s is not supported. GCS encryption is implicit and thus this parameter is not supported', $encryption), 'GoogleObjectStorageController::validateApi', __LINE__, TRUE);
    }
    $storage_class = trim(strtoupper($storage_class));
    if ($storage_class && $storage_class != self::STORAGE_CLASS_STANDARD && $storage_class != self::STORAGE_CLASS_DRA) {
      $validated = FALSE;
      self::log(sprintf('storage_class %s is not valid. Valid options are: %s or %s', $storage_class, self::STORAGE_CLASS_STANDARD, self::STORAGE_CLASS_DRA), 'GoogleObjectStorageController::validateApi', __LINE__, TRUE);
    }
    return $validated;
  }
  
}
?>
