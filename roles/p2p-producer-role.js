'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _get = function get(object, property, receiver) { if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { return get(parent, property, receiver); } } else if ("value" in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } };

var _roles = require('./roles');

var _roles2 = _interopRequireDefault(_roles);

var _p2pForwarder = require('../forwarder/p2p-forwarder.js');

var _p2pForwarder2 = _interopRequireDefault(_p2pForwarder);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var ProducerRole = function (_Role) {
	_inherits(ProducerRole, _Role);

	function ProducerRole(initId, config, sockets) {
		_classCallCheck(this, ProducerRole);

		var _this = _possibleConstructorReturn(this, Object.getPrototypeOf(ProducerRole).call(this, initId, config, sockets));

		if (_this.isP2PProducer()) {
			_this.forwarder = new _p2pForwarder2.default(_this.config);
		}
		return _this;
	}

	_createClass(ProducerRole, [{
		key: 'isMe',
		value: function isMe() {
			return this.isP2PProducer();
		}
	}, {
		key: 'onStart',
		value: function onStart(prev) {
			var _this2 = this;

			if (prev && prev.hello_sent) {
				return _get(Object.getPrototypeOf(ProducerRole.prototype), 'onStart', this).call(this, prev);
			}

			var defer = _q2.default.defer();
			var message = this.getHelloMessage();

			this.sockets.broadcast.send(new Buffer(message), 0, message.length, this.config.broadcastPort, this.getBroadcastAddress(), function (err) {
				if (err) {
					_this2.logger.warn(err);
					return defer.reject(err);
				} else {
					defer.resolve({
						hello_sent: true
					});
				}
			});

			return defer.promise;
		}
	}, {
		key: 'handleNewDestination',
		value: function handleNewDestination(msg) {
			this.forwarder.addForwaringInfo(msg.topic, msg.dest);
			var defer = _q2.default.defer();
			defer.resolve();
			return defer.promise;
		}
	}, {
		key: 'handleTReconfig',
		value: function handleTReconfig(msg) {
			var defer = _q2.default.defer();
			var msg = this.getSubscribeMessage();
			this.respondTo(msg, msg).then(function () {
				defer.resolve(msg);
			}, function (err) {
				return defer.reject(err);
			});
			return defer.promise;
		}
	}]);

	return ProducerRole;
}(_roles2.default);

exports.default = ProducerRole;