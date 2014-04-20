var countDownLatch = require('../util/countDownLatch');

var Event = function(app) {
	this.app = app;
};

module.exports = Event;

Event.prototype.start_all = function() {
	var self = this;
	var zookeeper = this.app.components.__zookeeper__;
	zookeeper.getChildren(watchServer.bind(this), function(children) {
		getServers(self.app, zookeeper, children);
	});
};

var getServers = function(app, zk, servers) {
	var results = [];
	if(!servers.length)	{
		return;
	}
	var latch = countDownLatch.createCountDownLatch(servers.length, function() {
		app.event.emit('replace_servers', results);
	});
	for(var i = 0; i < servers.length; i++) {
		(function(index) {
			zk.getData(zk.path + '/' + servers[index], function(err, data) {
				results.push(JSON.parse(data));
				latch.done();
			});
		})(i);
	}
};

var watchServer = function(event) {
	var self = this;
	var zookeeper = this.app.components.__zookeeper__;
	zookeeper.getChildren(watchServer.bind(this), function(children) {
		getServers(self.app, zookeeper, children);
	});
};