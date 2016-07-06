import chai from 'chai';
import spies from 'chai-spies';
import sinon from 'sinon'
import mockudp from 'mock-udp';

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js';
import Forwarder from '../src/forwarder/forwarder';
import { Socket } from 'dgram';
import NODE_TYPE from '../src/node-type';
import { MESSAGE_TYPE } from '../src/message-type';

let producerConf = {
	unicast: {
		port: 12556
	},
	roles: [NODE_TYPE.PRODUCER, NODE_TYPE.CONSUMER],
	producers: [{
		topic: 'foo',
		port: 10000
	}],
	consumers: [{
		topics: ['bar']
	}],
	monitoring: {
		subnet: '10.0.0.0/16'
	},
	logging: {
		disable: true
	}
};

describe('Hybrid (producer/consumer) role', () => {
	let broadcastScope;
	let unicastScope;
	let d;
	let socketSpy;
	let forwarderSpy;

	beforeEach(() => {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.10:1337');
		socketSpy = sinon.spy(Socket.prototype, 'send');
		forwarderSpy = sinon.spy(Forwarder.prototype, 'reconfig');
		d = new Daemon(producerConf, 12555);
	});

	afterEach(() => {
		Socket.prototype.send.restore();
		Forwarder.prototype.reconfig.restore();
		d.close();
	});

	it('Broadcasts hello message once on start', (done) => {
		d.hasStarted.then(() => {
			expect(socketSpy.callCount).to.equal(1);
			done();
		});
	});

	it('handles config message for both consumer and producer', (done) => {
		d.handleUnicastMessage({
			type: MESSAGE_TYPE.CONFIG,
			host: '10.0.0.10',
			port: 1337,
			monitoring: {
				host: '10.0.0.10',
				port: 2181
			}
		}).done(() => {
			expect(forwarderSpy.calledOnce).to.be.true;
			expect(forwarderSpy.getCall(0).args[0]).to.have.deep.property('monitoring.host', '10.0.0.10');
			expect(forwarderSpy.getCall(0).args[0]).to.have.deep.property('monitoring.port', 2181);

			//called once to broadcast hello (10.0.255.255:12555)
			expect(socketSpy.callCount).to.equal(1);

			expect(socketSpy.calledWith(
				sinon.match.any,
				sinon.match.any,
				sinon.match.any,
				12555,
				'10.0.255.255'
			)).to.be.true;

			done();
		});
	});

	// it('Handles reconfig message', (done) => {
	// 	d.hasStarted.then(() => {
	// 		d.handleBroadcastMessage({
	// 			type: 'reconfig',
	// 			host: '10.0.0.10',
	// 			port: 1337,
	// 			monitoring: {
	// 				host: '10.0.0.10',
	// 				port: 2181
	// 			}
	// 		});
	// 		expect(spy.calledOnce).to.be.true;
	// 		expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.host', '10.0.0.10');
	// 		expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.port', 2181);
	// 		done();
	// 	});
	// });
});