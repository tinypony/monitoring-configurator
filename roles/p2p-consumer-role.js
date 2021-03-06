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

var _nodeType = require('../node-type');

var _nodeType2 = _interopRequireDefault(_nodeType);

var _underscore = require('underscore');

var _underscore2 = _interopRequireDefault(_underscore);

var _kafkaPuller = require('../kafka/kafka-puller');

var _kafkaPuller2 = _interopRequireDefault(_kafkaPuller);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var P2PConsumerRole = function (_Role) {
	_inherits(P2PConsumerRole, _Role);

	function P2PConsumerRole(initId, config, sockets) {
		_classCallCheck(this, P2PConsumerRole);

		return _possibleConstructorReturn(this, Object.getPrototypeOf(P2PConsumerRole).call(this, initId, config, sockets));
	}

	_createClass(P2PConsumerRole, [{
		key: 'isMe',
		value: function isMe() {
			return this.isP2PConsumer();
		}
	}, {
		key: 'onStart',
		value: function onStart(prev) {
			var _this2 = this;

			if (prev && prev.hello_sent) {
				return _get(Object.getPrototypeOf(P2PConsumerRole.prototype), 'onStart', this).call(this);
			}

			var defer = _q2.default.defer();
			var message = this.getHelloMessage();

			this.broadcast(message).then(function () {
				_this2.logger.info('[p2p-Consumer] Broacasted hello');
				defer.resolve({
					hello_sent: true
				});
			}, function (err) {
				return defer.reject(err);
			});

			return defer.promise;
		}
	}, {
		key: 'handleTReconfig',
		value: function handleTReconfig(msg) {
			this.logger.info('[p2p-Consumer] handleTReconfig(' + msg + ')');
			var defer = _q2.default.defer();
			var response = this.getSubscribeMessage();
			this.logger.info('[p2p-Consumer] subscribe with ' + response);
			this.respondTo(msg, response).then(function () {
				defer.resolve(msg);
			}, function (err) {
				return defer.reject(err);
			});
			return defer.promise;
		}
	}]);

	return P2PConsumerRole;
}(_roles2.default);

exports.default = P2PConsumerRole;