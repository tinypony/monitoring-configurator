'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _get = function get(object, property, receiver) { if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { return get(parent, property, receiver); } } else if ("value" in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } };

var _roles = require('./roles');

var _roles2 = _interopRequireDefault(_roles);

var _q = require('q');

var _q2 = _interopRequireDefault(_q);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var ConsumerRole = function (_Role) {
	_inherits(ConsumerRole, _Role);

	function ConsumerRole(initId, config, sockets) {
		_classCallCheck(this, ConsumerRole);

		return _possibleConstructorReturn(this, Object.getPrototypeOf(ConsumerRole).call(this, initId, config, sockets));
	}

	_createClass(ConsumerRole, [{
		key: 'isMe',
		value: function isMe() {
			return this.isConsumer();
		}
	}, {
		key: 'onStart',
		value: function onStart(prev) {
			var _this2 = this;

			if (prev && prev.hello_sent) {
				return _get(Object.getPrototypeOf(ConsumerRole.prototype), 'onStart', this).call(this);
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
		key: 'configureClient',
		value: function configureClient(msg) {
			var _this3 = this;

			var defer = _q2.default.defer();
			this.config.monitoring = _underscore2.default.extend(this.config.monitoring, msg.monitoring);

			if (!this.isValidPort(msg.port)) {
				this.logger.warn('trying to send subscription message to an invalid port');
				defer.reject();
				return defer.promise;
			}

			var subscribeMsg = this.getSubscribeMessage();

			this.sockets.unicast.send(new Buffer(subscribeMsg), 0, subscribeMsg.length, msg.port, msg.host, function (e) {
				if (e) {
					_this3.logger.warn(JSON.stringify(e));
					return defer.reject(e);
				}
				defer.resolve(msg);
			});

			return defer.promise;
		}
	}, {
		key: 'handleConfig',
		value: function handleConfig(msg) {
			return this.configureClient(msg);
		}
	}, {
		key: 'handleReconfig',
		value: function handleReconfig(msg) {
			return this.configureClient(msg);
		}
	}]);

	return ConsumerRole;
}(_roles2.default);

exports.default = ConsumerRole;