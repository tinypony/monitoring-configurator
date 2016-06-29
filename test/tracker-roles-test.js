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

	const helloMessageProducer = {
		type: MESSAGE_TYPE.HELLO,
		roles: [ NODE_TYPE.P2PPRODUCER ],
		uuid: '1234567890',
		host: '10.0.0.10',
		publish: ['foo', 'bar'],
		subscribe: [],
		port: 6666
	};

	const helloMessageConsumer = {
		type: MESSAGE_TYPE.HELLO,
		roles: [ NODE_TYPE.P2PCONSUMER ],
		uuid: '1234567890',
		host: '10.0.0.10',
		publish: [],
		subscribe: [{
			port: 1234,
			topics: ['foo']
		}],
		port: 6666
	};

	const subscribeMessage = {
		type: MESSAGE_TYPE.SUBSCRIBE,
		host: '10.0.0.10',
		port: 6666,
		subscribe: [{
			port: 1234,
			topics: ['foo']
		}]
	};

	const publishMessage = {
		type: MESSAGE_TYPE.PUBLISH,
		host: '10.0.0.10',
		port: 6666,
		publish: ['foo', 'bar']
	}

	const consumerEndpoint = { host: helloMessageConsumer.host, port: helloMessageConsumer.subscribe[0].port, protocol: 'udp'};
	const producerSource = { host: '10.0.0.10', port: 6666 };

	let sockets;
	let tracker;
	let flushQueueSpy;

	beforeEach(() => {
		sockets = {
			unicast: {},
			broadcast: {}
		};

		flushQueueSpy = sinon.spy(Tracker.prototype, 'flushQueue');
		tracker = new Tracker(null, trackerConf, sockets);
	});

	afterEach(() => {
		flushQueueSpy.restore();
	});

	it('handles hello from producer by saving producer info and topics it writes to', done => {
		tracker.handleHello(helloMessageProducer).done(() => {
			expect(tracker.producers.length).to.eql(1);
			expect(tracker.producers[0].host).to.eql('10.0.0.10');
			expect(tracker.producers[0].port).to.eql(6666);
			expect(tracker.producers[0].topics).to.eql(['foo', 'bar']);
			done();
		});
	});

	it('handles publish from producer by saving producer info and topics it writes to', done => {
		tracker.handlePublish(publishMessage).done(() => {
			expect(tracker.producers.length).to.eql(1);
			expect(tracker.producers[0].host).to.eql('10.0.0.10');
			expect(tracker.producers[0].port).to.eql(6666);
			expect(tracker.producers[0].topics).to.eql(['foo', 'bar']);
			done();
		});
	});

	it('handles hello from consumer by saving it with default protocol', done => {
		tracker.handleHello(helloMessageConsumer).done(() => {
			expect(tracker.consumers['foo'].length).to.eql(1);
			expect(tracker.consumers['foo'][0]).to.eql(consumerEndpoint);
			done();
		});
	});

	it('notifies a registered producer with a new consumer endpoint', done => {
		tracker.producers = [ {host: '10.0.0.10', port: 6666, topics: ['foo', 'bar']} ];
		tracker.handleHello(helloMessageConsumer).done(() => {
			expect(tracker.newDestinationFIFO.length).to.eql(1);
			
			let item = tracker.newDestinationFIFO.shift();
			expect(item.source).to.eql(producerSource);
			expect(item.dest).to.eql(consumerEndpoint);
			expect(item.topic).to.eql('foo');

			done();
		});
	});

	it('sends registered consumer endpoint to a new producer', done => {
		tracker.consumers = {
			'foo': [ consumerEndpoint ]
		};

		tracker.handleHello(helloMessageProducer).done(() => {
			expect(tracker.newDestinationFIFO.length).to.eql(1);
			
			let item = tracker.newDestinationFIFO.shift();
			expect(item.source).to.eql(producerSource);
			expect(item.dest).to.eql(consumerEndpoint);
			expect(item.topic).to.eql('foo');

			done();
		});
	});
});
