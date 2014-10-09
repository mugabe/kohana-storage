<?php defined('SYSPATH') or die('No direct script access.');

use Aws\S3\S3Client;

/**
 * AWS S3 driver for Storage Module
 *
 * @package		Storage
 * @category	Base
 * @license		MIT
 */
class Kohana_Storage_Connection_S3 extends Storage_Connection
{
    /**
     * Default config
     *
     * AWS access credentials are located in the AWS Portal. Reference "config/storage.php" for
     * information on config.
     *
     * @access	protected
     * @var		array
     */
    protected $_config = array
    (
        'key'					=> NULL,
        'secret'				=> NULL,
        'bucket'				=> NULL,
        'public'				=> FALSE,
    );

    /**
     * AmazonS3 from AWS SDK
     *
     * @access	protected
     * @var		AmazonS3
     */

    /**
     * @var S3Client
     */
    protected $_driver;

    /**
     * Default S3 URL
     *
     * @access	protected
     * @var		string
     */
    protected $_url = '.s3.amazonaws.com/';

    /**
     * Load connection
     *
     * @access	protected
     * @return	AmazonS3
     */
    protected function _load()
    {
        if ($this->_driver === NULL)
        {
            $this->_driver = S3Client::factory(Arr::extract($this->_config,
                array('key', 'secret')));
        }

        return $this->_driver;
    }

    /**
     * Write content to file. If file already exists, it will be overwritten.
     *
     * @access    protected
     * @param    string
     * @param    resource
     * @param    string
     * @return    void
     */
    protected function _set($path, $handle, $mime)
    {
        $this->_load();

        $this->_driver->putObject(array(
            'ACL' => $this->_config['public'] ? 'public-read' : 'private',
            'Bucket' => $this->_config['bucket'],
            'Key' => $path,
            'Body' => $handle,
            'ContentType' => $mime
        ));
        $this->_driver->waitUntil('ObjectExists', array(
            'Bucket' => $this->_config['bucket'],
            'Key'    => $path
        ));

        return true;
    }

    /**
     * Read contents of file.
     *
     * @access    protected
     * @param    string
     * @param    resource
     * @return    bool
     */
    protected function _get($path, $handle)
    {
        $this->_load();

        $this->_driver->getObject(array(
            'Bucket' => $this->_config['bucket'],
            'Key' => $path,
            'SaveAs' => $handle
        ));

        return true;
    }

    /**
     * Delete
     *
     * @access    protected
     * @param    string
     * @return    void
     */
    protected function _delete($path)
    {
        $this->_load();

        $this->_driver->deleteObject(array(
            'Bucket' => $this->_config['bucket'],
            'Key' => $path,
        ));
    }

    /**
     * File size
     *
     * @access    protected
     * @param    string
     * @return    int
     */
    protected function _size($path)
    {
        $this->_load();

        $result = $this->_driver->headObject(array(
            'Bucket' => $this->_config['bucket'],
            'Key' => $path
        ));

        return $result['ContentLength'];
    }

    /**
     * Whether or not file exists
     *
     * @access    protected
     * @param    string
     * @return    bool
     */
    protected function _exists($path)
    {
        $this->_load();

        $result = $this->_driver->headObject(array(
            'Bucket' => $this->_config['bucket'],
            'Key' => $path
        ));

        return $result['DeleteMarker'];
    }

    /**
     * Get URL
     *
     * @access    protected
     * @param    string
     * @param    string
     * @return    string
     */
    protected function _url($path, $protocol)
    {
        $this->_load();

        return $this->_driver->getObjectUrl($this->_config['bucket'], $path);
    }

    /**
     * Get list based on path
     *
     * @access    protected
     * @param    string
     * @return    mixed
     */
    protected function _listing($path, $listing)
    {
        $this->_load();

        $result = $this->_driver->listObjects(array(
            'Bucket' => $this->_config['bucket'],
            'Prefix' => $path,
        ));

        foreach ($result->Contents as $file)
        {
            $name = (string) $file['Key'];

            $object = Storage_File::factory($name, $this)
                ->size((int) $file['Size'])
                ->modified(strtotime($file['LastModified']));

            $listing->set($object);
        }

        return $listing;
    }
}
