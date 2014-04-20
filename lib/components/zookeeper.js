var util = require('util');
var crypto = require('crypto');
var utils = require('../util/utils');
var zookeeper = require('node-zookeeper-client');
var Event = zookeeper.Event;
var CreateMode = zookeeper.CreateMode;
var EventEmitter = require('events').EventEmitter;
var logger = require('pomelo-logger').getLogger('pomelo-zookeeper', __filename);

module.exports = function(app, opts) {
	var component =  new Component(app, opts);
	return component;
};

var Component = function(app, opts) {
  this.app = app;
  this.hosts = opts.server || '127.0.0.1:2181';
  this.path = opts.path || '/pomelo/servers';
  this.username = opts.username || 'pomelo';
  this.password = opts.password || 'pomelo';
  this.setACL = opts.setACL;
  this.nodePath = this.path + '/' + this.app.serverId;
  this.onDataSet = false;
  this.authentication = this.username + ':' + this.password;
  var shaDigest = crypto.createHash('sha1').update(this.authentication).digest('base64');
  this.acls = [
    new zookeeper.ACL(
      zookeeper.Permission.ALL,
      new zookeeper.Id('digest', this.username + ':' + shaDigest)
    )
  ];

  this.client = zookeeper.createClient(this.hosts, {sessionTimeout: opts.timeout|| 5000});

  var self = this;
  this.client.once('connected', function() {
    self.client.addAuthInfo('digest', new Buffer(self.authentication));
    if(self.setACL) {
      self.client.setACL(self.path, self.acls, -1, function(error, stat) {
        if(error) {
          logger.warn('Failed to set ACL: %s.', error);
          return;
        }
        logger.info('ACL is set to: %j', self.acls);
      });
    }
  });

  this.client.connect();
};

var pro = Component.prototype;

pro.name = '__zookeeper__';

pro.start = function(cb) {
	if(this.app.serverType === 'master') {
		this.createNode(this.path, null, CreateMode.PERSISTENT, function(err, result) {
			if(!!err)	{
				logger.error('create master node failed! err : %j', err);
       utils.invokeCallback(cb, err);
       return;
     }
     utils.invokeCallback(cb);
   });
  } else {
    var buffer = new Buffer(JSON.stringify(this.app.getCurServer()));
    this.createNode(this.nodePath, buffer, CreateMode.EPHEMERAL, function(err, path) {
      if(!!err)	{
       logger.error('create server node failed! err : %j', err);
       utils.invokeCallback(cb, err);
     }
     utils.invokeCallback(cb);
   });
  }
};

pro.stop = function(force, cb) {
	this.client.close();
  utils.invokeCallback(cb);
};

pro.createNode = function(path, value, mode, cb) {
	var self = this;
  self.client.exists(path, function(err, stat) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }
    if(!stat) {
      self.client.create(path, value, mode, function(err, result) {
        utils.invokeCallback(cb, err, result);
        return;
      });
    } else {
      utils.invokeCallback(cb);
      return;
    }
  });
};

pro.getData = function(path, cb) {
  this.client.getData(path, function(err, data) {
    if(!!err) {
      logger.error('get data failed for server : %s', this.serverId);
      utils.invokeCallback(cb, err);
      return;
    }
    utils.invokeCallback(cb, null, data.toString());
  });
};

pro.getChildren = function(fun, cb) {
  var self = this;
	this.client.getChildren(this.path, fun, function(error, children, stats) {
		cb(children);
	});
};