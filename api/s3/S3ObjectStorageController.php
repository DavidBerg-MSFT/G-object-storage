<?php
/**
 * ObjectStorageController implementation for the S3 API
 */
class S3ObjectStorageController extends ObjectStorageController {
  // default API endpoint for S3
  const DEFAULT_S3_ENDPOINT = 's3.amazonaws.com';
  const DEFAULT_S3_REGION = 'us-east-1';
  const SIGNATURE_DATE_FORMAT = 'D, d M Y H:i:s T';
  
  // complete API endpoint URL (e.g. https://s3.amazonaws.com)
  private $api_url;
  // whether to use DNS based bucket referencing
  private $dns_containers;
  
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
      self::log(sprintf('GET Service request completed - status %d. Authentication was%s successful', $result['status'][0], $success ? '' : ' not'), 'S3ObjectStorageController::authenticate', __LINE__);
    }
    else self::log(sprintf('GET Service request failed'), 'S3ObjectStorageController::authenticate', __LINE__, TRUE);
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
      $xml = '<CompleteMultipartUpload>';
      $added = TRUE;
      $uploadId = NULL;
      foreach(array_keys($results['response']) as $i) {
        if (isset($results['response'][$i]['etag']) && isset($results['urls'][$i]) && preg_match('/partNumber=([0-9]+).*uploadId=([^&]+)/', $results['urls'][$i], $m)) {
          $part = $m[1];
          $uploadId = $m[2];
          self::log(sprintf('Got etag %s and uploadId %s for part %d', $results['response'][$i]['etag'], $uploadId, $part), 'S3ObjectStorageController::completeMultipartUpload', __LINE__);
          $xml .= sprintf('<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>', $part, $results['response'][$i]['etag']);
        }
        else $added = FALSE;
      }
      if ($added && $uploadId) {
        $xml .= '</CompleteMultipartUpload>';
        $headers = array('content-type' => 'text/plain', 'date' => gmdate(self::SIGNATURE_DATE_FORMAT));
        $params = array('uploadId' => $uploadId);
        $headers['Authorization'] = $this->sign('POST', $headers, $container, $object, $params);
        $request = array('method' => 'POST', 'url' => $this->getUrl($container, $object, $params), 'headers' => $headers, 'body' => $xml);
        $success = NULL;
        if ($result = $this->curl(array($request))) {
          $success = $result['status'][0] == 200;
          self::log(sprintf('Complete multipart upload request %s with status code %d for object %s/%s', $success ? 'completed successfully' : 'failed', $result['status'][0], $container, $object), 'S3ObjectStorageController::completeMultipartUpload', __LINE__, !$success);
        }
        else self::log(sprintf('Complete multipart upload request failed'), 'S3ObjectStorageController::completeMultipartUpload', __LINE__, TRUE);
      }
      else self::log(sprintf('Failed to retrieve required etag and upload IDs'), 'S3ObjectStorageController::completeMultipartUpload', __LINE__, TRUE);
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
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container), 'headers' => $headers);
    $exists = NULL;
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] == 200;
      self::log(sprintf('HEAD Bucket request completed - status %d. Bucket %s does%s exist', $result['status'][0], $container, $exists ? '' : ' not'), 'S3ObjectStorageController::containerExists', __LINE__);
    }
    else self::log(sprintf('HEAD Bucket request failed'), 'S3ObjectStorageController::containerExists', __LINE__, TRUE);
    return $exists;
  }
  
  /**
   * creates $container. returns TRUE on success, FALSE on failure
   * @param string $container the container to create
   * @param string $storage_class optional service specific storage class to 
   * apply to the container
   * @return boolean
   */
  public function createContainer($container, $storage_class=NULL) {
    $xml = $this->api_region != self::DEFAULT_S3_REGION ? sprintf('<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>%s</LocationConstraint></CreateBucketConfiguration>', $this->api_region) : NULL;
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    if ($xml) $headers['content-type'] = 'text/plain';
    $headers['Authorization'] = $this->sign('PUT', $headers, $container);
    $request = array('method' => 'PUT', 'url' => $this->getUrl($container, NULL, NULL, FALSE), 'headers' => $headers);
    if ($xml) $request['body'] = $xml;
    $created = NULL;
    if ($result = $this->curl(array($request))) {
      $created = $result['status'][0] == 200;
      self::log(sprintf('PUT Bucket %s request completed for region %s - status %d. Bucket %s %s', $container, $this->api_region, $result['status'][0], $container, $created ? 'created successfully' : 'could not be created'), 'S3ObjectStorageController::createContainer', __LINE__, !$created);
    }
    else self::log(sprintf('PUT Bucket %s request failed for region %s', $container, $this->api_region), 'S3ObjectStorageController::createContainer', __LINE__, TRUE);
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
      self::log(sprintf('DELETE Bucket request completed - status %d', $result['status'][0], $container), 'S3ObjectStorageController::deleteContainer', __LINE__);
    }
    else self::log(sprintf('DELETE Bucket request failed'), 'S3ObjectStorageController::deleteContainer', __LINE__, TRUE);
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
      self::log(sprintf('DELETE Object request completed - status %d', $result['status'][0], $container), 'S3ObjectStorageController::deleteObject', __LINE__);
    }
    else self::log(sprintf('DELETE Object request failed'), 'S3ObjectStorageController::deleteObject', __LINE__, TRUE);
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
      self::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist. %s', $result['status'][0], $container, $object, $exists ? '' : ' not', $size ? 'Size is ' . $size . ' bytes' : ''), 'S3ObjectStorageController::getObjectSize', __LINE__);
    }
    else self::log(sprintf('HEAD Object request failed'), 'S3ObjectStorageController::getObjectSize', __LINE__, TRUE);
    return $size;
  }
  
  /**
   * Returns the S3 API URL to use for the specified $container and $object
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
    $success = FALSE;
    
    // determine region
    if (!$this->api_endpoint && !$this->api_region) $this->api_region = self::DEFAULT_S3_REGION;
		foreach(explode("\n", file_get_contents(dirname(__FILE__) . '/region-mappings.ini')) as $line) {
			if (substr(trim($line), 0, 1) == '#') continue;
			if (preg_match('/^([^=]+)=(.*)$/', trim($line), $m)) {
				if (in_array($this->api_region, explode(',', $m[1]))) {
					$this->api_region = trim($m[2]);
					break;
				}
			}
		}
    self::log(sprintf('Set S3 API region to %s', $this->api_region), 'S3ObjectStorageController::init', __LINE__);
    
    // determine endpoint
		if (!$this->api_endpoint && $this->api_region) {
			foreach(explode("\n", file_get_contents(dirname(__FILE__) . '/region-endpoints.ini')) as $line) {
				if (substr(trim($line), 0, 1) == '#') continue;
				if (preg_match('/^([^=]+)=(.*)$/', trim($line), $m) && $m[1] == $this->api_region) $this->api_endpoint = trim($m[2]);
			}
		}
		if (!$this->api_endpoint) $this->api_endpoint = self::DEFAULT_S3_ENDPOINT;
		$this->api_url = preg_match('/^http/i', $this->api_endpoint) ? $this->api_endpoint : ($this->api_ssl ? 'https://' : 'http://') . $this->api_endpoint;
    self::log(sprintf('Set S3 API URL to %s', $this->api_url), 'S3ObjectStorageController::init', __LINE__);
    $this->api_ssl = preg_match('/^https/', $this->api_url) ? TRUE : FALSE;
    
		if ($this->api_key && $this->api_secret) {
		  $this->dns_containers = getenv('bm_param_dns_containers') !== '0';
		  $success = TRUE;
	  }
		else self::log(sprintf('Unable to initiate S3 object because api_key or api_secret parameters are not present'), 'S3ObjectStorageController::init', __LINE__, TRUE);

    return $success;
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
      self::log(sprintf('Initialized download for %s/%s. Returning URL %s and signature %s', $container, $object, $request['url'], $request['headers']['Authorization']), 'S3ObjectStorageController::initDownload', __LINE__);
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
   *                  {size}  replaced with the byte size of the file/part
   *                  {part}  replaced with incrementing numeric value 
   *                           corresponding with the part number
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
    if ($encryption && trim(strtolower($encryption)) == 'aes256') $request['headers']['x-amz-server-side-encryption'] = 'AES256';
    if ($storage_class && trim(strtolower($storage_class)) == 'reduced_redundancy') $request['headers']['x-amz-storage-class'] = 'REDUCED_REDUNDANCY';
    // multipart
    if ($parts > 1) {
      $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
      $params = array('uploads' => '');
      // encryption and storage class headers go into the initiation request - not the part requests
      if (isset($request['headers']['x-amz-server-side-encryption'])) {
        $headers['x-amz-server-side-encryption'] = 'AES256';
        unset($request['headers']['x-amz-server-side-encryption']);
      }
      if (isset($request['headers']['x-amz-storage-class'])) {
        $headers['x-amz-storage-class'] = 'REDUCED_REDUNDANCY';
        unset($request['headers']['x-amz-storage-class']);
      }
      $headers['Authorization'] = $this->sign('POST', $headers, $container, $object, $params);
      $req = array('method' => 'POST', 'url' => $this->getUrl($container, $object, $params), 'headers' => $headers);
      $uploadId = NULL;
      if ($result = $this->curl(array($req), TRUE)) {
        if ($result['status'][0] == 200 && isset($result['body'][0]) && preg_match('/uploadid>([^<]+)<\//si', $result['body'][0], $m)) {
          $uploadId = $m[1];
          self::log(sprintf('Multipart upload initiated successfully - UploadId: %s', $uploadId), 'S3ObjectStorageController::initUpload', __LINE__);
        }
        else self::log(sprintf('Unable to initiate multipart upload - status %s', $result['status'][0]), 'S3ObjectStorageController::initUpload', __LINE__, TRUE);
        
        $url = $uploadId ? $this->getUrl($container, $object, $params = array('partNumber' => '{part}', 'uploadId' => $uploadId)) : NULL;
        $request['url'] = $url;
        $baseHeaders = $request['headers'];
        $request['headers'] = array();
        for($i=1; $i<=$parts; $i++) {
          $headers = $baseHeaders;
          $headers['Authorization'] = $this->sign('PUT', $headers, $container, $object, array('partNumber' => $i, 'uploadId' => $uploadId));
          $request['headers'][] = $headers;
        }
      }
      else self::log(sprintf('Unable to initiate multipart upload - unknown error'), 'S3ObjectStorageController::initUpload', __LINE__, TRUE);
    }
    else {
      $request['url'] = $url;
      $signature = $this->sign('PUT', $request['headers'], $container, $object, $params);
      $request['headers']['Authorization'] = $signature;
      self::log(sprintf('Got AWS S3 authorization signature "%s" for string "%s"', $signature, str_replace("\n", '\n', $string)), 'S3ObjectStorageController::initUpload', __LINE__);
    }
    return $request;
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
    $headers = array('date' => gmdate(self::SIGNATURE_DATE_FORMAT));
    $headers['Authorization'] = $this->sign('HEAD', $headers, $container, $object);
    $request = array('method' => 'HEAD', 'url' => $this->getUrl($container, $object), 'headers' => $headers);
    $exists = NULL;
    if ($result = $this->curl(array($request))) {
      $exists = $result['status'][0] == 200;
      self::log(sprintf('HEAD Object request completed - status %d. Object %s/%s does%s exist', $result['status'][0], $container, $object, $exists ? '' : ' not'), 'S3ObjectStorageController::objectExists', __LINE__);
    }
    else self::log(sprintf('HEAD Object request failed'), 'S3ObjectStorageController::objectExists', __LINE__, TRUE);
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
    // add amz headers to signature
    $amz_headers = array();
    foreach($headers as $key => $val) {
      if (preg_match('/^x-amz/', $key)) $amz_headers[strtolower($key)] = $val;
    }
    ksort($amz_headers);
    $amz_string = '';
    foreach($amz_headers as $key => $val) $amz_string .= $key . ':' . trim($val) . "\n";
    
    $uri = '';
    if ($object) $uri = $container . '/' . $object;
    else if ($method == 'PUT' && !$object) $uri = $container;
    else if ($container) $uri = $container . '/';
    $string = sprintf("%s\n\n%s\n%s\n%s/%s", 
                      strtoupper($method),
                      isset($headers['content-type']) ? $headers['content-type'] : '',
                      $headers['date'], 
                      $amz_string,
                      $uri);
    if ($params) {
      ksort($params);
      $started = FALSE;
      foreach($params as $key => $val) {
        if (in_array($key, array('acl', 'lifecycle', 'location', 'logging', 'notification', 'partNumber', 'policy', 'requestPayment', 'torrent', 'uploadId', 'uploads', 'versionId', 'versioning', 'versions', 'website'))) {
          $string .= ($started ? '&' : '?') . $key . ($val ? '=' . $val : '');
          $started = TRUE;
        }
      }
    }
    self::log(sprintf('Signing string %s', str_replace("\n", '\n', $string)), 'S3ObjectStorageController::sign', __LINE__);
		$signature = base64_encode(extension_loaded('hash') ? hash_hmac('sha1', $string, $this->api_secret, TRUE) : pack('H*', sha1((str_pad($this->api_secret, 64, chr(0x00)) ^ (str_repeat(chr(0x5c), 64))) . pack('H*', sha1((str_pad($this->api_secret, 64, chr(0x00)) ^ (str_repeat(chr(0x36), 64))) . $string)))));
		return sprintf('AWS %s:%s', $this->api_key, $signature);
  }
  
}
?>
