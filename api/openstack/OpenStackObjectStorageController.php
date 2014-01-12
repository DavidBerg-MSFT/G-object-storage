<?php
/**
 * ObjectStorageController implementation for the OpenStack API
 */
class OpenStackObjectStorageController extends ObjectStorageController {
  // password auth type
  const AUTH_TYPE_PASSWORD = 'passwordCredentials';
  // access key auth type
  const AUTH_TYPE_ACCESS_KEY = 'apiAccessKeyCredentials';
  // Rackspace API key auth type
  const AUTH_TYPE_RAX_API_KEY = 'RAX-KSKEY:apiKeyCredentials';
  // default service type
  const DEFAULT_SERVICE_TYPE = 'object-store';
  // min multipart upload size is 1MB
  const MIN_MULTIPART_UPLOAD_SIZE = 1048576;
  // internal endpoint URL type
  const URL_TYPE_INTERNAL = 'internal';
  // public endpoint URL type
  const URL_TYPE_PUBLIC = 'public';
  
  // auth type and keys
  private $api_auth_type;
  private $key_for_api_key;
  private $key_for_api_secret;
  
  // service type identifier
  private $api_service_type;
  
  // tenant ID
  private $api_tenant_id;
  
  // tenant name
  private $api_tenant_name;
  
  // API token (retrieved during authentication)
  private $api_token;
  
  // URL type
  private $api_url_type;
  
  // file used to store names of multipart objects created
  private $multipart_tracker_file;
  
  // tracks the auth token
  private $auth_token_file;
  
  // Service endpoint URL
  private $service_endpoint;
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method should reference the instance
   * attributes $api_key, $api_secret, $api_endpoint and $api_region as 
   * necessary to complete the authentication
   * @return boolean
   */
  protected function authenticate() {
    // authenticate using the $api_endpoint and lookup the service endpoint
    // using the api_region, api_service_type and api_url_type parameters
    
    // check for existing token
    if (file_exists($this->auth_token_file) && is_array($auth = json_decode(file_get_contents($this->auth_token_file), TRUE)) && isset($auth['api_token']) && isset($auth['service_endpoint'])) {
      self::log(sprintf('Got auth token from cache file %s', $this->auth_token_file), 'OpenStackObjectStorageController::authenticate', __LINE__);
      $this->api_token = $auth['api_token'];
      $this->service_endpoint = $auth['service_endpoint'];
      return TRUE;
    }
    
    $auth = array('auth' => array());
    $auth['auth'][$this->api_auth_type] = array();
    $auth['auth'][$this->api_auth_type][$this->key_for_api_key] = $this->api_key;
    // placeholder for logging
    $auth['auth'][$this->api_auth_type][$this->key_for_api_secret] = 'xxx';
    if ($this->api_tenant_id) $auth['auth']['tenantId'] = $this->api_tenant_id;
    else if ($this->api_tenant_name) $auth['auth']['tenantName'] = $this->api_tenant_name;
    self::log(sprintf('Attempting authentication using request: %s', json_encode($auth)), 'OpenStackObjectStorageController::authenticate', __LINE__);
    $auth['auth'][$this->api_auth_type][$this->key_for_api_secret] = $this->api_secret;
    $headers = array('Accept' => 'application/json', 'Content-Type' => 'application/json');
    $request = array('method' => 'POST', 'url' => $this->api_endpoint, 'headers' => $headers, 'body' => json_encode($auth));
    $success = NULL;
    if ($result = $this->curl(array($request), TRUE)) {
      $success = $result['status'][0] == 200 || $result['status'][0] == 203;
      self::log(sprintf('Authentication complete - status %d. Authentication was%s successful', $result['status'][0], $success ? '' : ' not'), 'OpenStackObjectStorageController::authenticate', __LINE__, !$success);
      if ($success) {
        $success = FALSE;
        if ($response = json_decode($result['body'][0], TRUE)) {
          self::log(sprintf('Successfully decoded json response. Getting token and service endpoint'), 'OpenStackObjectStorageController::authenticate', __LINE__);
          if (isset($response['access']['token']['id'])) {
            $this->api_token = $response['access']['token']['id'];
            self::log(sprintf('Got authentication token - expires %s', $response['access']['token']['expires']), 'OpenStackObjectStorageController::authenticate', __LINE__);
            if (isset($response['access']['serviceCatalog'])) {
              foreach($response['access']['serviceCatalog'] as $service) {
                if (isset($service['type']) && $service['type'] == $this->api_service_type) {
                  if (isset($service['endpoints']) && is_array($service['endpoints']) && count($service['endpoints'])) {
                    self::log(sprintf('Evaluating %s service %s with %d endpoints', $service['type'], $service['name'], count($service['endpoints'])), 'OpenStackObjectStorageController::authenticate', __LINE__);
                    foreach($service['endpoints'] as $endpoint) {
                      if (!$this->api_region || $this->api_region == $endpoint['region']) {
                        $urls = array();
                        foreach(array_keys($endpoint) as $key) {
                          if (preg_match('/^(.*)URL$/i', $key, $m) && preg_match('/http/', $endpoint[$key])) {
                            $urls[$m[1]] = $endpoint[$key];
                            self::log(sprintf('Added %s URL %s for endpoint %s and region %s', $m[1], $endpoint[$key], $endpoint['tenantId'], $endpoint['region']), 'OpenStackObjectStorageController::authenticate', __LINE__);
                          }
                        }
                        if (isset($urls[$this->api_url_type])) {
                          $this->service_endpoint = $urls[$this->api_url_type];
                          self::log(sprintf('Successfully obtained service endpoint URL %s for region %s. Authentication Successful', $this->service_endpoint, $endpoint['region']), 'OpenStackObjectStorageController::authenticate', __LINE__);
                          $success = TRUE;
                          break;
                        }
                        else self::log(sprintf('Endpoint does not have URL of type %s', $this->api_url_type), 'OpenStackObjectStorageController::authenticate', __LINE__);
                      }
                      else self::log(sprintf('Skipping endpoint because the region %s does not match api_region %s', $endpoint['region'], $this->api_region), 'OpenStackObjectStorageController::authenticate', __LINE__);
                    }
                    if ($success) break;
                  }
                  else self::log(sprintf('Evaluating %s service %s has no endpoints', $service['type'], $service['name']), 'OpenStackObjectStorageController::authenticate', __LINE__, TRUE);
                }
                else self::log(sprintf('Skipping service of type %s', $service['type']), 'OpenStackObjectStorageController::authenticate', __LINE__);
              }
            }
            else self::log(sprintf('Unable to get serviceCatalog from authentication response: %s', $result['body'][0]), 'OpenStackObjectStorageController::authenticate', __LINE__, TRUE);
          }
          else self::log(sprintf('Unable to get access token from authentication response: %s', $result['body'][0]), 'OpenStackObjectStorageController::authenticate', __LINE__, TRUE);
        }
        else self::log(sprintf('Unable to json decode authentication response: %s', $result['body'][0]), 'OpenStackObjectStorageController::authenticate', __LINE__, TRUE);
      }
    }
    else self::log(sprintf('Authentication request failed'), 'OpenStackObjectStorageController::authenticate', __LINE__, TRUE);
    
    // cache token
    if ($success) {
      $fp = fopen($this->auth_token_file, 'w');
      fwrite($fp, json_encode(array('api_token' => $this->api_token, 'service_endpoint' => $this->service_endpoint)));
      fclose($fp);
      self::log(sprintf('Cached auth token in cache file %s', $this->auth_token_file), 'OpenStackObjectStorageController::authenticate', __LINE__);
    }
    
    return $success;
  }
  
  /**
   * Invoked following completion of a multipart upload. return TRUE if 
   * the multipart upload was successful, FALSE if it failed
   * @param string $container the container uploaded to
   * @param string $object the name of the object uploaded
   * @param array $results return value from invoking the 'curl' method
   * @return boolean
   */
  public function completeMultipartUpload($container, $object, $results) {
    $success = NULL;
    if (isset($results['response']) && is_array($results['response']) && count($results['response']) > 0 && isset($results['urls']) && is_array($results['urls']) && count($results['response']) == count($results['urls'])) {
      $manifest = array();
      $added = TRUE;
      foreach(array_keys($results['response']) as $i) {
        if (isset($results['response'][$i]['etag']) && isset($results['urls'][$i]) && preg_match('/p([0-9]+)$/', $results['urls'][$i], $m)) {
          $part = $m[1];
          self::log(sprintf('Got etag %s for part %d (size=%d)', $results['response'][$i]['etag'], $part, $results['request'][$i]['Content-Length']), 'OpenStackObjectStorageController::completeMultipartUpload', __LINE__);
          $manifest[] = array('path' => sprintf('/%s/%s.p%d', $container, $object, $part), 'etag' => $results['response'][$i]['etag'], 'size_bytes' => $results['request'][$i]['Content-Length']);
        }
        else $added = FALSE;
      }
      self::log(sprintf('Created static multipart object manifest for %s/%s: %s', $container, $object, json_encode($manifest)), 'OpenStackObjectStorageController::completeMultipartUpload', __LINE__);
      if ($added) {
        $headers = array('content-type' => 'application/json', 'X-Auth-Token' => $this->api_token);
        $request = array('method' => 'PUT', 'url' => $this->getUrl($container, $object, array('multipart-manifest' => 'put')), 'headers' => $headers, 'body' => json_encode($manifest));
        $success = NULL;
        if ($result = $this->curl(array($request))) {
          $success = $result['status'][0] >= 200 && $result['status'][0] < 300;
          self::log(sprintf('Complete multipart upload request %s with status code %d for object %s/%s', $success ? 'completed successfully' : 'failed', $result['status'][0], $container, $object), 'OpenStackObjectStorageController::completeMultipartUpload', __LINE__, !$success);
          if ($success) exec(sprintf('echo "%s" >> %s', $object, $this->multipart_tracker_file));
        }
        else self::log(sprintf('Complete multipart upload request failed'), 'OpenStackObjectStorageController::completeMultipartUpload', __LINE__, TRUE);
      }
      else self::log(sprintf('Failed to retrieve required etag and upload IDs'), 'OpenStackObjectStorageController::completeMultipartUpload', __LINE__, TRUE);
    }
    return $success;
  }
  
  /**
   * returns TRUE if $container exists, FALSE otherwise. return NULL on 
   * error
   * @param string $container the container to check 
   * @return boolean
   */
  public function containerExists($container) {
    $headers = array('X-Auth-Token' => $this->api_token);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container), 'headers' => $headers);
    $exists = NULL;
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] >= 200 && $result['status'][0] < 300;
      self::log(sprintf('HEAD Container request completed - status %d. Container %s does%s exist', $result['status'][0], $container, $exists ? '' : ' not'), 'OpenStackObjectStorageController::containerExists', __LINE__);
    }
    else self::log(sprintf('HEAD Container request failed'), 'OpenStackObjectStorageController::containerExists', __LINE__, TRUE);
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
    $headers = array('X-Auth-Token' => $this->api_token);
    $request = array('method' => 'PUT', 'url' => $this->getUrl($container), 'headers' => $headers);
    $created = NULL;
    if ($result = $this->curl(array($request))) {
      $created = $result['status'][0] == 201 || $result['status'][0] == 202;
      self::log(sprintf('PUT Container %s request completed - status %d. Container %s %s', $container, $result['status'][0], $container, $created ? 'created successfully' : 'could not be created'), 'OpenStackObjectStorageController::createContainer', __LINE__, !$created);
    }
    else self::log(sprintf('PUT Container %s request failed', $container), 'OpenStackObjectStorageController::createContainer', __LINE__, TRUE);
    return $created;
  }
  
  /**
   * deletes $container. returns TRUE on success, FALSE on failure
   * @param string $container the container to delete 
   * @return boolean
   */
  public function deleteContainer($container) {
    $headers = array('X-Auth-Token' => $this->api_token);
    $request = array('method' => 'DELETE', 'url' => $this->getUrl($container), 'headers' => $headers);
    $deleted = NULL;
    if ($result = $this->curl(array($request))) {
      $deleted = $result['status'][0] >= 200 && $result['status'][0] < 300;
      self::log(sprintf('DELETE Container %s request completed - status %d. Container %s', $container, $result['status'][0], $deleted ? 'deleted successfully' : 'could not be deleted'), 'OpenStackObjectStorageController::deleteContainer', __LINE__, !$deleted);
    }
    else self::log(sprintf('DELETE Container %s request failed', $container), 'OpenStackObjectStorageController::deleteContainer', __LINE__, TRUE);
    return $deleted;
  }
  
  /**
   * deletes $object. returns TRUE on success, FALSE on failure
   * @param string $container the object container
   * @param string $object name of the object to delete
   * @return boolean
   */
  public function deleteObject($container, $object) {
    $object = str_replace('.p{part}', '', $object);
    $headers = array('X-Auth-Token' => $this->api_token);
    
    // check if object is multipart manifest file - delete entire manifest
    $params = NULL;
    if (file_exists($this->multipart_tracker_file)) {
      foreach(file($this->multipart_tracker_file) as $test) {
        if (trim($test) == $object) {
          $params = array('multipart-manifest' => 'delete');
          break;
        }
      }
    }
    
    $request = array('method' => 'DELETE', 'url' => $this->getUrl($container, $object, $params), 'headers' => $headers);
    $deleted = NULL;
    if ($result = $this->curl(array($request))) {
      $deleted = $result['status'][0] >= 200 && $result['status'][0] < 300;
      self::log(sprintf('DELETE Object %s/%s request completed - status %d. Object %s', $container, $object, $result['status'][0], $deleted ? 'deleted successfully' : 'could not be deleted'), 'OpenStackObjectStorageController::deleteObject', __LINE__, !$deleted);
    }
    else self::log(sprintf('DELETE Object %s/%s request failed', $container, $object), 'OpenStackObjectStorageController::deleteObject', __LINE__, TRUE);
    return $deleted;
  }
  
  /**
   * returns the size of $object in bytes. return NULL on error
   * @param string $container the object container
   * @param string $object name of the object
   * @return int
   */
  public function getObjectSize($container, $object) {
    $headers = array('X-Auth-Token' => $this->api_token);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $size = NULL;
    if ($result = $this->curl(array($request))) {
      if (($exists = $result['status'][0] >= 200 && $result['status'][0] < 300) && isset($result['response'][0]['content-length'])) $size = $result['response'][0]['content-length'];
      self::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist. %s', $result['status'][0], $container, $object, $exists ? '' : ' not', $size ? 'Size is ' . $size . ' bytes' : ''), 'OpenStackObjectStorageController::getObjectSize', __LINE__);
    }
    else self::log(sprintf('HEAD Object request failed'), 'OpenStackObjectStorageController::getObjectSize', __LINE__, TRUE);
    return $size;
  }
  
  /**
   * Returns the Azure API URL to use for the specified $container and $object
   * @param string $container the container to return the URL for
   * @param string $object optional object to include in the URL
   * @param array $params optional URL parameters
   * @return string
   */
  private function getUrl($container=NULL, $object=NULL, $params=NULL) {
    $url = $this->service_endpoint;
    if ($container) $url = sprintf('%s%s%s', $url, '/' . $container, $object ? '/' . $object : '');
    if (is_array($params)) {
      foreach(array_keys($params) as $i => $param) {
        $url .= ($i ? '&' : '?') . $param . ($params[$param] ? '=' . $params[$param] : '');
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
    // auth type
    $this->api_auth_type = getenv('bm_param_api_auth_type');
    if (!$this->api_auth_type) $this->api_auth_type = self::AUTH_TYPE_PASSWORD;
    
    // determine auth keys
    switch($this->api_auth_type) {
      case self::AUTH_TYPE_PASSWORD:
        $this->key_for_api_key = 'username';
        $this->key_for_api_secret = 'password';
        break;
      case self::AUTH_TYPE_ACCESS_KEY:
        $this->key_for_api_key = 'accessKey';
        $this->key_for_api_secret = 'secretKey';
        break;
      case self::AUTH_TYPE_RAX_API_KEY:
        $this->key_for_api_key = 'username';
        $this->key_for_api_secret = 'apiKey';
        break;
    }
    
    // service type identifier
    $this->api_service_type = getenv('bm_param_api_service_type');
    if (!$this->api_service_type) $this->api_service_type = self::DEFAULT_SERVICE_TYPE;
    // tenant ID
    $this->api_tenant_id = getenv('bm_param_api_tenant_id');
    // tenant name
    $this->api_tenant_name = getenv('bm_param_api_tenant_name');
    // endpoint URL type
    $this->api_url_type = getenv('bm_param_api_url_type');
    if (!$this->api_url_type) $this->api_url_type = self::URL_TYPE_PUBLIC;
    
    // identity service endpoint
    if ($this->api_endpoint && !preg_match('/^http/', $this->api_endpoint)) $this->api_endpoint = 'http' . ($this->api_ssl ? 's' : '') . '://' . $this->api_endpoint;
    if ($this->api_endpoint && !preg_match('/tokens$/', $this->api_endpoint)) $this->api_endpoint .= (preg_match('/\/$/', $this->api_endpoint) ? '' : '/') . 'tokens';
    
    $this->multipart_tracker_file = sprintf('%s/.openstack-multipart-objects', getenv('bm_run_dir'));
    $this->auth_token_file = sprintf('%s/.openstack-auth-token', getenv('bm_run_dir'));
    
    self::log(sprintf('Initialization complete. api_auth_type: %s; api_service_type: %s; api_tenant_id: %s; api_tenant_name: %s; api_url_type: %s; key_for_api_key: %s; key_for_api_secret: %s; api_endpoint: %s', 
              $this->api_auth_type, $this->api_service_type, $this->api_tenant_id, $this->api_tenant_name, $this->api_url_type, $this->key_for_api_key, $this->key_for_api_secret, $this->api_endpoint), 
              'OpenStackObjectStorageController::cleanupContainer', __LINE__);
    
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
      $request = array('headers' => array('X-Auth-Token' => $this->api_token));
      $request['url'] = $this->getUrl($container, $object);
      self::log(sprintf('Initialized download for %s/%s. Returning URL %s', $container, $object, $request['url']), 'OpenStackObjectStorageController::initDownload', __LINE__);
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
    $request = array();
    $request['headers'] = array('content-type' => self::CONTENT_TYPE, 'X-Auth-Token' => $this->api_token);
    $request['url'] = $this->getUrl($container, $object) . ($parts > 1 ? '.p{part}' : '');
    self::log(sprintf('Initialized %s upload for %s/%s. Returning URL %s', $parts > 1 ? 'multipart' : 'non-multipart', $container, $object, $request['url']), 'OpenStackObjectStorageController::initUpload', __LINE__);
    return $request;
  }
  
  /**
   * may be overridden to define a minimum segment size in bytes for multipart 
   * uploads (default is 5 MB). If NULL, no minimum size constraint will be 
   * applied
   * @return int
   */
  protected function multipartMinSegment() {
    return self::MIN_MULTIPART_UPLOAD_SIZE;
  }
  
  /**
   * may be overridden if multipart uploads are supported
   */
  protected function multipartSupported() {
    return TRUE;
  }
  
  /**
   * returns TRUE if the object identified by $name exists in $container. 
   * return NULL on error
   * @param string $container the container to check
   * @param string $object the name of the object
   * @return boolean
   */
  public function objectExists($container, $object) {
    $headers = array('X-Auth-Token' => $this->api_token);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $exists = NULL;
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] >= 200 && $result['status'][0] < 300;
      self::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist', $result['status'][0], $container, $object, $exists ? '' : ' not'), 'OpenStackObjectStorageController::objectExists', __LINE__);
    }
    else self::log(sprintf('HEAD Object request failed'), 'OpenStackObjectStorageController::objectExists', __LINE__, TRUE);
    return $exists;
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
    
    // encryption is not supported
    if ($encryption) {
      $validated = FALSE;
      self::log(sprintf('encryption is not supported by this API'), 'OpenStackObjectStorageController::validateApi', __LINE__, TRUE);
    }
    
    // storage_class is not supported
    $storage_class = trim(strtoupper($storage_class));
    if ($storage_class) {
      $validated = FALSE;
      self::log(sprintf('storage_class is not supported by this API'), 'OpenStackObjectStorageController::validateApi', __LINE__, TRUE);
    }
    
    // validate auth type
    if ($validated && $this->api_auth_type != self::AUTH_TYPE_PASSWORD && $this->api_auth_type != self::AUTH_TYPE_ACCESS_KEY && $this->api_auth_type != self::AUTH_TYPE_RAX_API_KEY) {
      $validated = FALSE;
      self::log(sprintf('api_auth_type %s is not valid. Valid options are %s; %s; %s', $this->api_auth_type, self::AUTH_TYPE_PASSWORD, self::AUTH_TYPE_ACCESS_KEY, self::AUTH_TYPE_RAX_API_KEY), 'OpenStackObjectStorageController::validateApi', __LINE__, TRUE);
    }
    
    // validate endpoint URL type
    if ($validated && $this->api_url_type != self::URL_TYPE_PUBLIC && $this->api_url_type != self::URL_TYPE_INTERNAL) {
      $validated = FALSE;
      self::log(sprintf('api_url_type %s is not valid. Valid options are %s; %s', $this->api_url_type, self::URL_TYPE_PUBLIC, self::URL_TYPE_INTERNAL), 'OpenStackObjectStorageController::validateApi', __LINE__, TRUE);
    }
    
    return $validated;
  }
  
}
?>
