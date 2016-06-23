import Tracker from '../src/roles/tracker';
import { MESSAGE_TYPE } from '../src/message-type';
import NODE_TYPE from '../src/node-type';
import { expect } from 'chai';
import sinon from 'sinon'

describe('p2p Tracker', () => {
	let trackerConf = {
		unicast: {
			port: 12556
		},
		roles: [NODE_TYPE.TRACKER],
		monitoring: {
			subnet: '10.0.0.0/16'
		},
		logging: {
			disable: true
		}
	};
	let sockets;
	let tracker;

	beforeEach(() => {
		sockets = {
			unicast: {},
			broadcast: {}
		};
		tracker = new Tracker(null, trackerConf, sockets);
	});

	afterEach(() => {

	});

	it('handles hello from producer by saving producer info and topics it writes to', (done) => {
		var helloMessage = {
			type: MESSAGE_TYPE.HELLO,
			roles: [NODE_TYPE.P2PPRODUCER],
			uuid: '1234567890',
			host: '10.0.0.10',
			publish: ['foo', 'bar'],
			subscribe: [],
			port: 6666
		};

		tracker.handleHello(helloMessage).done(() => {
			expect(tracker.producers.length).to.eql(1);
			expect(tracker.producers[0].host).to.eql('10.0.0.10');
			expect(tracker.producers[0].port).to.eql(6666);
			expect(tracker.producers[0].topics).to.eql(['foo', 'bar']);
			done();
		});
	});

	
});
