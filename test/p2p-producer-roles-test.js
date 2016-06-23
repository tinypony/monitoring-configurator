import P2PProducer from '../src/roles/p2p-producer-role';
import { MESSAGE_TYPE } from '../src/message-type';
import NODE_TYPE from '../src/node-type';
import chai from 'chai';
import sinon from 'sinon'

describe('p2p Producer', () => {
	let producerConf = {
		unicast: {
			port: 12556
		},
		roles: [NODE_TYPE.PRODUCER],
		producers: [{
			topic: 'foo',
			port: 10000
		}],
		monitoring: {
			subnet: '10.0.0.0/16'
		},
		logging: {
			disable: true
		}
	};


	
});
