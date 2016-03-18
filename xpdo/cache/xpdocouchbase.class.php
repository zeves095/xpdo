<?php
/*
 * Copyright 2010-2015 by MODX, LLC.
 *
 * This file is part of xPDO.
 *
 * xPDO is free software; you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * xPDO is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * xPDO; if not, write to the Free Software Foundation, Inc., 59 Temple Place,
 * Suite 330, Boston, MA 02111-1307 USA
 */

/**
 * Provides a couchbase-powered xPDOCache implementation.
 *
 * This requires the couchbase extension for PHP.
 *
 * @package xpdo
 * @subpackage cache
 */
class xPDOCouchBase extends xPDOCache {
    /** @var CouchbaseCluster */
    protected $couchbase = null;
    /** @var CouchbaseBucket */
    protected $bucket;

    public function __construct(& $xpdo, $options = array()) {
        parent :: __construct($xpdo, $options);
        if (class_exists('CouchbaseCluster', true)) {
            $dsn = $this->getOption($this->key . '_couchbase_dsn', $options, $this->getOption('couchbase_dsn', $options, '127.0.0.1'));
            $username = $this->getOption($this->key . '_couchbase_username', $options, $this->getOption('couchbase_username', $options, ''));
            $password = $this->getOption($this->key . '_couchbase_password', $options, $this->getOption('couchbase_password', $options, ''));

            $this->couchbase = new CouchbaseCluster($dsn, $username, $password);
            $bucketName = $this->getOption($this->key . '_couchbase_bucket', $options, $this->key);
            $this->bucket = $this->couchbase->openBucket($bucketName, $this->getOption($this->key . '_couchbase_bucket_password', $options, ''));
            if ($this->bucket instanceof CouchbaseBucket) {
                $this->initialized = true;
                $this->bucket->setTranscoder(function($value) {
                    return array(json_encode($value), 0, 0);
                }, function($value, $flags, $datatype) {
                    return json_decode($value, true);
                });
            } else {
                $this->couchbase = null;
                $this->bucket = null;
                $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "xPDOCouchBase[{$this->key}]: Error creating couchbase provider for server: {$dsn}");
            }
        } else {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "xPDOCouchBase[{$this->key}]: Error creating couchbase provider; xPDOCouchBase requires the PHP couchbase extension.");
        }
    }

    public function add($key, $var, $expire= 0, $options= array()) {
        if ($expire > 0) {
            $options['expiry'] = $expire;
        }
        try {
            $this->bucket->insert($this->getCacheKey($key), $var, $options);
            return true;
        } catch (Exception $e) {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "xPDOCouchBase[{$this->key}]: Error adding cache item with key {$key}. {$e->getMessage()}");
        }
        return false;
    }

    public function set($key, $var, $expire= 0, $options= array()) {
        if ($expire > 0) {
            $options['expiry'] = $expire;
        }
        try {
            $this->bucket->upsert($this->getCacheKey($key), $var, $options);
            return true;
        } catch (Exception $e) {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "xPDOCouchBase[{$this->key}]: Error setting cache item with key {$key}. {$e->getMessage()}");
        }
        return false;
    }

    public function replace($key, $var, $expire= 0, $options= array()) {
        if ($expire > 0) {
            $options['expiry'] = $expire;
        }
        try {
            $this->bucket->replace($this->getCacheKey($key), $var, $options);
            return true;
        } catch (Exception $e) {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "xPDOCouchBase[{$this->key}]: Error replacing cache item with key {$key}. {$e->getMessage()}");
        }
        return false;
    }

    public function delete($key, $options= array()) {
        if (!isset($options['multiple_object_delete']) || empty($options['multiple_object_delete'])) {
            try {
                $this->bucket->remove($this->getCacheKey($key), $options);
                return true;
            } catch (Exception $e) {
                $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "xPDOCouchBase[{$this->key}]: Error deleting cache item with key {$key}. {$e->getMessage()}");
            }
        } else {
            try {
                $this->bucket->manager()->flush();
                return true;
            } catch (Exception $e) {
                $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "xPDOCouchBase[{$this->key}]: Error flushing cache due to delete request for key {$key}. {$e->getMessage()}");
            }
        }
        return false;
    }

    public function get($key, $options= array()) {
        try {
            return $this->bucket->get($this->getCacheKey($key), $options)->value;
        } catch (Exception $e) {
            $this->xpdo->log(xPDO::LOG_LEVEL_WARN, "xPDOCouchBase[{$this->key}]: Cache item with key {$key} does not exist. {$e->getMessage()}");
        }
        return null;
    }

    public function flush($options= array()) {
        try {
            $this->bucket->manager()->flush();
            return true;
        } catch (Exception $e) {
            $this->xpdo->log(xPDO::LOG_LEVEL_ERROR, "xPDOCouchBase[{$this->key}]: Error flushing cache bucket. {$e->getMessage()}");
        }
        return false;
    }
}
