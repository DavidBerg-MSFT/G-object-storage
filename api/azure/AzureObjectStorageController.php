<?php
/**
 * ObjectStorageController implementation for the Azure Cloud Storage (Azure) 
 * API
 */
class AzureObjectStorageController extends ObjectStorageController {
  // default API endpoint for Azure
  const DEFAULT_AZURE_ENDPOINT_PREFIX = 'blob.core.windows.net';
  const SIGNATURE_DATE_FORMAT = 'D, d M Y H:i:s T';
  // max multipart upload size is 4MB
  const MAX_MULTIPART_UPLOAD_SIZE = 4194304;
  // max upload size is 64MB
  const MAX_UPLOAD_SIZE = 67108864;
  // identifier for global redundant storage class
  const STORAGE_CLASS_GRS = 'GRS';
  // identifier for locally redundant storage class
  const STORAGE_CLASS_LRS = 'LRS';
  // api version to use
  const API_VERSION = '2011-08-18';
  
  // api endpoint url
  private $api_url;
  
  /**
   * invoked once during validation. Should return TRUE if authentication is 
   * successful, FALSE otherwise. this method should reference the instance
   * attributes $api_key, $api_secret, $api_endpoint and $api_region as 
   * necessary to complete the authentication
   * @return boolean
   */
  protected function authenticate() {
    // test authentication by listing containers
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
    $headers['Authorization'] = $this->sign('GET', $headers, NULL, NULL, $params = array('comp' => 'list'));
    $request = array('method' => 'GET', 'url' => $this->getUrl(NULL, NULL, $params), 'headers' => $headers);
    $success = NULL;
    if ($result = $this->curl(array($request))) {
      $success = $result['status'][0] == 200 || $result['status'][0] == 404;
      self::log(sprintf('GET Service request completed - status %d. Authentication was%s successful', $result['status'][0], $success ? '' : ' not'), 'AzureObjectStorageController::authenticate', __LINE__);
    }
    else self::log(sprintf('GET Service request failed'), 'AzureObjectStorageController::authenticate', __LINE__, TRUE);
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
      $xml = '<?xml version="1.0" encoding="utf-8"?>';
      $xml .= "\n<BlockList>\n";
      $added = TRUE;
      foreach(array_keys($results['response']) as $i) {
        if (isset($results['urls'][$i]) && preg_match('/blockid=([^&]+)/', $results['urls'][$i], $m)) {
          $part = $m[1];
          self::log(sprintf('Got blockid %s for part %d', $part, $i+1), 'AzureObjectStorageController::completeMultipartUpload', __LINE__);
          $xml .= sprintf("<Latest>%s</Latest>\n", $part);
        }
        else $added = FALSE;
      }
      if ($added) {
        $xml .= '</BlockList>';
        $headers = array('content-type' => 'text/plain; charset=UTF-8', 'date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
        $params = array('comp' => 'blocklist');
        $headers['Authorization'] = $this->sign('PUT', $headers, $container, $object, $params);
        $request = array('method' => 'PUT', 'url' => $this->getUrl($container, $object, $params), 'headers' => $headers, 'body' => $xml);
        $success = NULL;
        if ($result = $this->curl(array($request))) {
          $success = $result['status'][0] == 201;
          self::log(sprintf('Complete multipart upload request %s with status code %d for object %s/%s', $success ? 'completed successfully' : 'failed (xml: ' . $xml . ')', $result['status'][0], $container, $object), 'AzureObjectStorageController::completeMultipartUpload', __LINE__, !$success);
        }
        else self::log(sprintf('Complete multipart upload request failed using xml: %s', $xml), 'AzureObjectStorageController::completeMultipartUpload', __LINE__, TRUE);
      }
      else self::log(sprintf('Failed to retrieve parts'), 'AzureObjectStorageController::completeMultipartUpload', __LINE__, TRUE);
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
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container, NULL, $params = array('restype' => 'container'));
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, NULL, $params), 'headers' => $headers);
    $exists = NULL;
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] == 200;
      self::log(sprintf('HEAD Container request completed - status %d. Container %s does%s exist', $result['status'][0], $container, $exists ? '' : ' not'), 'AzureObjectStorageController::containerExists', __LINE__);
    }
    else self::log(sprintf('HEAD Container request failed'), 'AzureObjectStorageController::containerExists', __LINE__, TRUE);
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
    $headers = array('content-length' => 0, 'date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
    $headers['Authorization'] = $this->sign('PUT', $headers, $container, NULL, $params = array('restype' => 'container'));
    $request = array('method' => 'PUT', 'url' => $this->getUrl($container, NULL, $params), 'headers' => $headers);
    $created = NULL;
    if ($result = $this->curl(array($request))) {
      $created = $result['status'][0] == 201;
      self::log(sprintf('PUT Container %s request completed for region %s - status %d. Container %s %s', $container, $this->api_region, $result['status'][0], $container, $created ? 'created successfully' : 'could not be created'), 'AzureObjectStorageController::createContainer', __LINE__, !$created);
    }
    else self::log(sprintf('PUT Container %s request failed', $container), 'AzureObjectStorageController::createContainer', __LINE__, TRUE);
    return $created;
  }
  
  /**
   * deletes $container. returns TRUE on success, FALSE on failure
   * @param string $container the container to delete 
   * @return boolean
   */
  public function deleteContainer($container) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
    $headers['Authorization'] = $this->sign('DELETE', $headers, $container, NULL, $params = array('restype' => 'container'));
    $request = array('method' => 'DELETE', 'url' => $this->getUrl($container, NULL, $params), 'headers' => $headers);
    $deleted = NULL;
    if ($result = $this->curl(array($request))) {
      $deleted = $result['status'][0] == 202;
      self::log(sprintf('DELETE Container request completed - status %d', $result['status'][0], $container), 'AzureObjectStorageController::deleteContainer', __LINE__);
    }
    else self::log(sprintf('DELETE Container request failed'), 'AzureObjectStorageController::deleteContainer', __LINE__, TRUE);
    return $deleted;
  }
  
  /**
   * deletes $object. returns TRUE on success, FALSE on failure
   * @param string $container the object container
   * @param string $object name of the object to delete
   * @return boolean
   */
  public function deleteObject($container, $object) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
    $headers['Authorization'] = $this->sign('DELETE', $headers, $container, $object);
    $request = array('method' => 'DELETE', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $deleted = NULL;
    if ($result = $this->curl(array($request))) {
      $deleted = $result['status'][0] == 202;
      self::log(sprintf('DELETE Object request completed - status %d', $result['status'][0], $container), 'AzureObjectStorageController::deleteObject', __LINE__, !$deleted);
    }
    else self::log(sprintf('DELETE Object request failed'), 'AzureObjectStorageController::deleteObject', __LINE__, TRUE);
    return $deleted;
  }
  
  /**
   * returns the size of $object in bytes. return NULL on error
   * @param string $container the object container
   * @param string $object name of the object
   * @return int
   */
  public function getObjectSize($container, $object) {
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container, $object);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $size = NULL;
    if ($result = $this->curl(array($request))) {
      if (($exists = $result['status'][0] == 200) && isset($result['response'][0]['content-length'])) $size = $result['response'][0]['content-length'];
      self::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist. %s', $result['status'][0], $container, $object, $exists ? '' : ' not', $size ? 'Size is ' . $size . ' bytes' : ''), 'AzureObjectStorageController::getObjectSize', __LINE__);
    }
    else self::log(sprintf('HEAD Object request failed'), 'AzureObjectStorageController::getObjectSize', __LINE__, TRUE);
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
    $url = $this->api_url;
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
    
		$this->api_url = preg_match('/^http/i', $this->api_endpoint) ? $this->api_endpoint : ($this->api_ssl ? 'https://' : 'http://') . $this->api_key . '.' . self::DEFAULT_AZURE_ENDPOINT_PREFIX;
    self::log(sprintf('Set Azure API URL to %s', $this->api_url), 'AzureObjectStorageController::init', __LINE__);
    
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
      $request = array('headers' => array('date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION));
      $request['url'] = $this->getUrl($container, $object);
      $request['headers']['Authorization'] = $this->sign('GET', $request['headers'], $container, $object);
      self::log(sprintf('Initialized download for %s/%s. Returning URL %s and signature %s', $container, $object, $request['url'], $request['headers']['Authorization']), 'AzureObjectStorageController::initDownload', __LINE__);
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
    // multipart
    if ($parts > 1) {
      $url = $this->getUrl($container, $object, $params = array('comp' => 'block', 'blockid' => '{part_base64}'));
      $request = array();
      $request['url'] = $url;
      $request['headers'] = array('content-type' => self::CONTENT_TYPE, 'date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
      $request['headers']['Authorization'] = $this->sign('PUT', $request['headers'], $container, $object, array('comp' => 'block'));
      self::log(sprintf('Initialized multipart upload for %s/%s. Returning URL %s and signature %s', $container, $object, $request['url'], $request['headers']['Authorization']), 'AzureObjectStorageController::initUpload', __LINE__); 
    }
    else {
      $request = array();
      $request['headers'] = array('content-type' => self::CONTENT_TYPE, 'date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-blob-type' => 'BlockBlob', 'x-ms-version' => self::API_VERSION);
      $request['url'] = $this->getUrl($container, $object);
      $request['headers']['Authorization'] = $this->sign('PUT', $request['headers'], $container, $object);
      self::log(sprintf('Initialized non-multipart upload for %s/%s. Returning URL %s and signature %s', $container, $object, $request['url'], $request['headers']['Authorization']), 'AzureObjectStorageController::initUpload', __LINE__);
    }
    return $request;
  }
  
  /**
   * may be overridden to define a maximum segment size in bytes for multipart 
   * uploads. If NULL, no maximum size constraint will be applied
   * @return int
   */
  protected function multipartMaxSegment() {
    return self::MAX_MULTIPART_UPLOAD_SIZE;
  }
  
  /**
   * may be overridden to define a minimum segment size in bytes for multipart 
   * uploads (default is 5 MB). If NULL, no minimum size constraint will be 
   * applied
   * @return int
   */
  protected function multipartMinSegment() {
    return NULL;
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
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT), 'x-ms-version' => self::API_VERSION);
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container, $object);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $exists = NULL;
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] == 200;
      self::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist', $result['status'][0], $container, $object, $exists ? '' : ' not'), 'AzureObjectStorageController::objectExists', __LINE__);
    }
    else self::log(sprintf('HEAD Object request failed'), 'AzureObjectStorageController::objectExists', __LINE__, TRUE);
    return $exists;
  }
  
  /**
   * returns an authorization signature for the parameters specified
   * @param string $method the http method
   * @param array $headers http headers
   * @param string $container optional container
   * @param string $object optional object to create the signature for
   * @param array $params optional URL parameters
   * @return string
   */
  private function sign($method, $headers, $container=NULL, $object=NULL, $params=NULL) {
    // add x-ms headers to signature
    $ms_headers = array();
    foreach($headers as $key => $val) {
      if (preg_match('/^x-ms/', $key)) $ms_headers[strtolower($key)] = $val;
    }
    ksort($ms_headers);
    $ms_string = '';
    foreach($ms_headers as $key => $val) $ms_string .= $key . ':' . trim($val) . "\n";
    
    $uri = '/' . $this->api_key . '/';
    if ($object) $uri .= $container . '/' . $object;
    else if (($method == 'PUT' || $method == 'HEAD') && !$object) $uri .= $container;
    else if ($container) $uri .= $container . '/';
    $string = sprintf("%s\n%s\n%s\n%s\n%s%s", 
                      strtoupper($method),
                      isset($headers['content-md5']) ? $headers['content-md5'] : '',
                      isset($headers['content-type']) ? $headers['content-type'] : '',
                      $headers['date'], 
                      $ms_string,
                      $uri);
    if ($params) {
      $started = FALSE;
      ksort($params);
      foreach($params as $key => $val) {
        // don't include some parameters in the signature
        if ($key == 'restype' || $key == 'blockid') continue;
        $string .= ($started ? '&' : '?') . $key . '=' . $val;
        $started = TRUE;
      }
    }
    self::log(sprintf('Signing string %s', str_replace("\n", '\n', $string)), 'AzureObjectStorageController::sign', __LINE__);
		$signature = base64_encode(hash_hmac('sha256', $string, base64_decode($this->api_secret), TRUE));
		return sprintf('SharedKeyLite %s:%s', $this->api_key, $signature);
  }
  
  /**
   * may be overridden to define a maximum single request upload size in bytes.
   * If NULL, no maximum size constraint will be applied
   * @return int
   */
  protected function uploadMaxSize() {
    return self::MAX_UPLOAD_SIZE;
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
    if ($encryption) {
      $validated = FALSE;
      self::log(sprintf('encryption parameter %s is not supported. Azure encryption is implicit and thus this parameter is not supported', $encryption), 'AzureObjectStorageController::validateApi', __LINE__, TRUE);
    }
    $storage_class = trim(strtoupper($storage_class));
    if ($storage_class && $storage_class != self::STORAGE_CLASS_LRS && $storage_class != self::STORAGE_CLASS_GRS) {
      $validated = FALSE;
      self::log(sprintf('storage_class %s is not valid. Valid options are: %s or %s', $storage_class, self::STORAGE_CLASS_LRS, self::STORAGE_CLASS_GRS), 'AzureObjectStorageController::validateApi', __LINE__, TRUE);
    }
    return $validated;
  }
  
}
?>
