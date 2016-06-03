'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});
var MESSAGE_TYPE = exports.MESSAGE_TYPE = {
	HELLO: 'hello',
	CONFIG: 'config',
	RECONFIG: 'reconfig',
	SUBSCRIBE: 'subscribe',
	REGISTER_SLAVE: 'regslave',
	CLUSTER_RESIZE: 'clusterresize',
	UNSUBSCRIBE: 'unsubscribe'
};