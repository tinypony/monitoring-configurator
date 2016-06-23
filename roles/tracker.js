'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _roles = require('./roles');

var _roles2 = _interopRequireDefault(_roles);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _dequeue = require('dequeue');

var _dequeue2 = _interopRequireDefault(_dequeue);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var Tracker = function (_Role) {
	_inherits(Tracker, _Role);

	function Tracker(initId, config, sockets) {
		_classCallCheck(this, Tracker);

		var _this = _possibleConstructorReturn(this, Object.getPrototypeOf(Tracker).call(this, initId, config, sockets));

		_this.producers = []; //Array<{port:int, host:String, topics: Array<String>}>
		_this.consumers = {}; //Map<String, Array<{port:int, host:String}>>
		_this.newDestinationFIFO = new _dequeue2.default();
		return _this;
	}

	_createClass(Tracker, [{
		key: 'isMe',
		value: function isMe() {
			return this.isTracker();
		}
	}, {
		key: 'onStart',
		value: function onStart() {
			var _this2 = this;

			var defer = _q2.default.defer();
			var message = this.getTrackerReconfigureMessage();
			this.logger.info('[Tracker] onStart()');
			this.broadcast(message).then(function () {
				_this2.logger.info('[Tracker] Broadcasted tracker config');
				defer.resolve();
			}, function (err) {
				return defer.reject(err);
			});

			return defer.promise;
		}
	}, {
		key: 'wasProducer',
		value: function wasProducer(msg) {
			return msg.roles && _underscore2.default.contains(msg.roles, NODE_TYPE.P2PPRODUCER);
		}
	}, {
		key: 'wasConsumer',
		value: function wasConsumer(msg) {
			return msg.roles && _underscore2.default.contains(msg.roles, NODE_TYPE.P2PCONSUMER);
		}
	}, {
		key: 'enhanceWithHost',
		value: function enhanceWithHost(host, endpoints) {
			return _underscore2.default.map(endpoints, function (ep) {
				return _underscore2.default.extend({}, ep, { host: host });
			});
		}
	}, {
		key: 'handleHello',
		value: function handleHello(msg) {
			var defer = _q2.default.defer();
			this.logger.info('[Tracker] handleHello( ' + JSON.stringify(msg) + ' )');
			try {
				if (this.wasProducer(msg)) {
					this.logger.info('Hello from p2p-producer');
					this.registerProducer(msg.host, msg.port, msg.publish);
				}

				if (this.wasConsumer(msg)) {
					this.logger.info('Hello from p2p-consumer');
					this.registerConsumer(this.enhanceWithHost(msg.host, msg.subscribe));
				}

				if (!this.wasConsumer(msg) && !this.wasProducer(msg)) {
					this.logger.info('was not a procuder nor a consumer');
				}
			} catch (e) {
				this.logger.warn(e);
				this.logger.warn(e.message);
			}

			defer.resolve(msg);
			return defer.promise;
		}
	}, {
		key: 'handleSubscribe',
		value: function handleSubscribe(msg) {
			this.registerConsumer(this.enhanceWithHost(msg.host, msg.subscribe));
		}
	}, {
		key: 'handlePublish',
		value: function handlePublish(msg) {
			this.registerProducer(msg.host, msg.port, msg.publish);
		}
	}, {
		key: 'registerProducer',
		value: function registerProducer(host, port, writeTopics) {
			var _this3 = this;

			var source = {
				host: host,
				port: port,
				topics: writeTopics
			};

			this.producers.push(source);

			var consumersSubset = _underscore2.default.pick(this.consumers, writeTopics);
			_underscore2.default.each(consumersSubset, function (endpoints, topic) {
				_underscore2.default.each(endpoints, function (endpoint) {
					_this3.notifyProducer(source, topic, endpoint);
				});
			});
		}
	}, {
		key: 'addTopicEndpointMapping',
		value: function addTopicEndpointMapping(topic, endpoint) {
			var is_new = arguments.length <= 2 || arguments[2] === undefined ? false : arguments[2];

			this.logger.info('[Tracker] addTopicEndpointMapping( ' + JSON.stringify(topic) + ', ' + JSON.stringify(endpoint) + ')');

			if (is_new) {
				this.consumers[topic] = [endpoint];
			} else {
				this.consumers[topic].push(endpoint);
			}

			this.notifyProducers(topic, endpoint);
		}
	}, {
		key: 'notifyProducers',
		value: function notifyProducers(topic, endpoint) {
			var _this4 = this;

			this.logger.info('[Tracker] notifyProducers( ' + JSON.stringify(topic) + ', ' + JSON.stringify(endpoint) + ')');

			topicWriters = _underscore2.default.filter(this.producers, function (p) {
				return _underscore2.default.contains(p.topics, topic);
			});

			_underscore2.default.each(topicWriters, function (source) {
				_this4.notifyProducer(source, topic, endpoint);
			});
		}
	}, {
		key: 'notifyProducer',
		value: function notifyProducer(source, topic, dest) {
			this.logger.info('[Tracker] notifyProducer( ' + JSON.stringify(source) + ', ' + JSON.stringify(topic) + ', ' + JSON.stringify(dest) + ')');

			this.newDestinationFIFO.push({
				dest: dest,
				topic: topic,
				source: { host: source.host, port: parseInt(source.port) }
			});

			if (this.newDestinationFIFO.length === 1) {
				setImmediate(this.flushQueue.bind(this));
			}
		}
	}, {
		key: 'flushQueue',
		value: function flushQueue() {
			var _this5 = this;

			var _loop = function _loop() {
				var item = _this5.newDestinationFIFO.shift();
				var msg = _this5.getNewDestinationMessage(item.topic, item.dest);
				_this5.sockets.unicast.send(new Buffer(msg), 0, msg.length, item.source.port, item.source.host, function () {
					_this5.logger.info('Send topic to endpoint mapping ' + JSON.stringify(item.topic) + ' -> ' + JSON.stringify(item.dest));
				});
			};

			while (this.newDestinationFIFO.length) {
				_loop();
			}
		}

		/**
   * subscription.topics
   * subscription.host
   * subscription.port
   *
   */

	}, {
		key: 'registerConsumer',
		value: function registerConsumer(subscriptions) {
			var _this6 = this;

			this.logger.info('[Tracker] registerConsumer(' + JSON.stringify(subscriptions) + ')');

			_underscore2.default.each(subscriptions, function (sub) {
				var endpoint = { host: sub.host, port: parseInt(sub.port), protocol: sub.protocol ? sub.protocol : 'udp' };

				_underscore2.default.each(sub.topics, function (t) {
					if (!_this6.consumers[t] || !_this6.consumers[t].length) {
						_this6.addTopicEndpointMapping(t, endpoint, true);
						return;
					}

					var existing = _underscore2.default.findWhere(_this6.consumers[t], endpoint);
					if (!existing) {
						_this6.addTopicEndpointMapping(t, endpoint);
					} else {
						_this6.notifyProducers(t, endpoint);
					}
				});
			});
		}
	}]);

	return Tracker;
}(_roles2.default);

exports.default = Tracker;