/**
 * Downloads files from Amazon S3 storage and maps parameter to local files.
 * @module gsf-amazon-s3-parameter-mapper
 */
var EventEmitter = require('events');
var defaultConfig = require('./config.json');
var log = require('gsf-server-logger').logger;
var aws = require('aws-sdk');
var fs = require('fs');
var fse = require('fs-extra');
var https = require('https');
var path = require('path');
var async = require('async');
var cluster = require('cluster');
var fileProtocol = 's3://';
var url = require('url');
var ProxyAgent = require('proxy-agent');

var cancelTranslateEvent = 'cancelTranslate';

/**
 * @class
 * @implements {ParameterMapper}
 * @param {GSFAmazonS3ParameterMapperConfig} rootConfig - The configuration that the module
 *   will load.
 */
function AmazonS3ParameterMapper(rootConfig) {
  var dataDir, s3;
  var config = Object.assign({}, defaultConfig, rootConfig);
  var cancelEvents = new EventEmitter();

  // Create a directory (dataDir) to which we will copy data.
  dataDir =  path.resolve(config.dataDirectory);
  var workerDir = cluster.isMaster ? 'master' : cluster.worker.id.toString();
  dataDir = path.join(dataDir,
    config.nodeInfo.nodeAddress,
    config.nodeInfo.nodePort.toString(),
    workerDir);
  createDirectory(dataDir);

  // Inherits docs from ParameterMapper interface
  this.init = function() {
    // Setup S3 object with access keys.

    var agentOptions = {};
    var Agent;

    // Create the httpOptions object if it doesn't exist.
    if (!config.httpOptions) {
      config.httpOptions = {};
    }

    if (config.httpsCerts) {
      // If we have an httpCerts bundle, load it into a custom https agent.
      var certs = [
        fs.readFileSync(config.httpsCerts)
      ];
      agentOptions.ca = certs;
      agentOptions.rejectUnauthorized = true;
      // Set the agent to the http agent
      Agent = https.Agent;
    }

    if (config.proxy) {
      // Add proxy url values to the agent options
      Object.assign(agentOptions, url.parse(config.proxy));
      // Set the agent to the proxy agent
      Agent = ProxyAgent;
    }

    if (Agent) {
      config.httpOptions.agent = new Agent(agentOptions);
    }

    s3 = new aws.S3(config);
  };

  // Inherits docs from ParameterMapper interface
  this.translate = function(name, value, callback) {
    log.trace('Entered AmazonS3ParameterMapper.translate()', name, value);
    var translated = false;
    if (typeof value === 'string' && value.toLowerCase().indexOf(fileProtocol) === 0) {
      log.trace('Calling handleS3', value);
      translated = handleS3(value, callback);
      log.trace('HandleS3 returned', translated);
    }
    return translated;
  };

  this.cancelTranslate = function(kill, callback) {
    log.trace('AmazonS3ParameterMapper: sending cancel translate event');
    cancelEvents.emit(cancelTranslateEvent);
    process.nextTick(callback);
  };

  // Inherits docs from ParameterMapper interface
  this.reverseTranslate = function() {
    return false;
  };

  // Inherits docs from ParameterMapper interface
  this.postJobCleanup = function(callback) {
    // Only run post job cleanup code if module is configured to do so
    if (config.postJobCleanup) {
      // DeleteDirectory calls back with err or null if successful
      deleteDirectory(dataDir, callback);
    } else {
      callback(null);
    }
  };

  /**
   * Recursively creates a directory and any parent directories.
   * This is only called on server startup to make sure we are able to create the directory
   * and fail fast if we can't. Otherwise the directory is recreated at the beginning of each
   * job run in the function downloadS3File().
   */
  function createDirectory(path) {
    try {
      fse.ensureDirSync(path);
    } catch (e) {
      // Don't throw an error if the directory already exists.
      if (e.code !== 'EEXIST') {
        throw e;
      }
      log.debug('Folder ' + path + ' already exists, continuing');
    }
  }

  /**
   * Recursively deletes a directory and all contents.
   */
  function deleteDirectory(path, callback) {
    log.debug('Attempting to delete directory: ', path);
    // Fse.remove calls back with err or null if successful
    fse.remove(path, callback);
  }

  /**
   * Checks for file existence synchronously.
   * @param filename {string} - The file to check.
   * @returns {boolean} Returns True, if file exists.
   */
  function fileExists(filename) {
    try {
      fs.accessSync(filename);
      return true;
    } catch (e) {
      return false;
    }
  }

  /**
   * Downloads a file from an Amazon S3 bucket.
   * @param bucket {string} - The S3 bucket to download from.
   * @param fileKey {string} - The file key to gain access.
   * @param outFile {string} - The output file to download to.
   */
  function downloadS3File(bucket, fileKey, outFile, cb) {
    log.trace('AmazonS3ParameterMapper downloading: ', fileKey);
    // Check to see if any directories need to be created.
    fse.ensureDir(path.dirname(outFile), function(err) {
      if (err) {
        cb('Error making directory.', err);
      } else {
        var fileStream = fs.createWriteStream(outFile);
        var params = {
          Bucket: bucket,
          Key   : fileKey,
          RequestPayer: 'requester'
        };
        // Download file.
        var stream = s3.getObject(params).createReadStream();
        stream.pipe(fileStream);
        var cancelTranslate = function() {
          log.trace('AmazonS3ParameterMapper canceling: ', fileKey);
          try {
            stream.unpipe(fileStream);
            fileStream.end();
          } catch (err) {
            cb('Error closing stream while canceling');
            return;
          }
          cb('Amazon S3 parameter mapper translate canceled');
        };
        cancelEvents.once(cancelTranslateEvent, cancelTranslate);
        stream.once('end', function(err) {
          cancelEvents.removeListener(cancelTranslateEvent, cancelTranslate);
          log.trace('AmazonS3ParameterMapper finished: ', fileKey);
          cb(err);
        });
        stream.once('error', function(err) {
          cancelEvents.removeListener(cancelTranslateEvent, cancelTranslate);
          log.trace('stream received error event');
          fse.removeSync(outFile);
          cb(err);
        });
        fileStream.once('error', function(err) {
          cancelEvents.removeListener(cancelTranslateEvent, cancelTranslate);
          log.trace('write stream received error event');
          fse.removeSync(outFile);
          cb(err);
        });
      }
    });
  }

  /**
   * Extracts useful information from S3 path.
   * @param inURL {string} - The url to parse for S3 reference.
   * @returns {object} Returns object containing folderKey, fileKey, bucket,
   *   folderName and basename.
   */
  function parseURL(inURL) {
    // Get bucket, key, folder, and basename info from URL.
    var parsedPath = path.parse(path.normalize(inURL));
    var splitDir = (parsedPath.dir).split(path.sep);
    var folderKey = (splitDir.length > 2) ? splitDir.slice(2,splitDir.length).join('/') + '/' : '';

    return {
      folderKey : folderKey,
      fileKey   : folderKey + parsedPath.base,
      bucket    : splitDir[1],
      folderName: splitDir[splitDir.length - 1],
      basename  : parsedPath.base
    };
  }

  /**
   * Function for downloading and mapping Amazon S3 parameters.
   * @param value {string} - The url to download.
   */
  function handleS3(value, callback) {
    // Parse URL to get bucket, file, key info.
    var inputURL = value;
    var pURL = parseURL(inputURL);

    // Check if the bucket name matches one of the patterns specified for this mapper
    if (config.buckets) {
      if (config.buckets.indexOf(pURL.bucket) === -1) {
        return false;
      }
    }

    
    // Verify that file exists and is readable.
    s3.headObject({Bucket: pURL.bucket, Key: pURL.fileKey,RequestPayer: 'requester'}, function(err, header) {
      if (err) {
        callback('Invalid S3 object: ' + err);
        return;
      }
      // Check configuration value so we know to dl one or all.
      if (config.downloadAllFilesInDirectory) {
        // Download all files in folder.
        var s3params = {Bucket: pURL.bucket, Prefix: pURL.folderKey,RequestPayer: 'requester'};
        s3.listObjects(s3params, function(err, data) {
          if (err) {
            callback(err);
            return;
          }
          var downloads = [];
          var outputFiles = [];
          cancelEvents.setMaxListeners(data.Contents.length);
          data.Contents.forEach(function(file) {
            var fileKey = file.Key;
            var outputFile = path.join(dataDir, fileKey);
            // We don't want directories.
            if (fileKey[fileKey.length - 1] !== '/') {
              outputFiles.push(outputFile);
              // Download if the file doesn't already exist or if the s3 modified date is more
              // recent than the local file modified time.
              if ((!fileExists(outputFile)) ||
                (file.LastModified.getTime() > fs.statSync(outputFile).mtime.getTime())) {
                downloads.push(function(cb) {
                  downloadS3File(pURL.bucket, fileKey, outputFile, cb);
                });
              } else {
                log.debug('Skipping download of file key: ',
                  outputFile, ' because it already exists locally',
                  ' and has not been modified on S3.');
              }
            }
          });
          async.parallel(downloads, function(err) {
            log.trace('S3 data downloaded to: ', dataDir);
            cancelEvents.removeAllListeners(cancelTranslateEvent);
            if (err) {
              outputFiles.forEach(function(outputFile) {
                fse.remove(outputFile, function(err) {
                  if (err) {
                    log.warn(err);
                  }
                });
              });
              process.nextTick(function() {
                callback(err);
              });
              return;
            }
            var newValue = path.join(dataDir, pURL.fileKey);
            callback(null, newValue);
          });
        });
      } else {
        // Download a single file.
        var outputFile = path.join(dataDir, pURL.fileKey);
        var modDate = new Date(header.LastModified);
        if ((!fileExists(outputFile)) ||
          (modDate.getTime() > fs.statSync(outputFile).mtime.getTime())) {
          cancelEvents.setMaxListeners(1);
          downloadS3File(pURL.bucket, pURL.fileKey, outputFile, function(err) {
            cancelEvents.removeAllListeners(cancelTranslateEvent);
            if (err) {
              fse.remove(outputFile, function(err) {
                if (err) {
                  log.warn(err);
                }
              });
              process.nextTick(function() {
                callback(err);
              });
              return;
            }
            log.trace('S3 data downloaded to: ', dataDir);
            callback(null, outputFile);
          });
        } else {
          callback(null, outputFile);
        }
      }
    });

    return true;
  }
}

module.exports = AmazonS3ParameterMapper;