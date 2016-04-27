import chai from 'chai';
import spies from 'chai-spies';
import sinon from 'sinon'
import mockudp from 'mock-udp';

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js';
import Forwarder from '../src/forwarder/forwarder';
import { Socket } from 'dgram';

let producerConf = {
	unicast: {
		port: 12556
	},
	roles: ['producer', 'consumer'],
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
	let spy;

	beforeEach(() => {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');

		spy = sinon.spy(Socket.prototype, 'send');
		d = new Daemon(producerConf, 12555);
	});

	afterEach(() => {
		Socket.prototype.send.restore();
		d.close();
	});

	it('Broadcasts hello message once on start', (done) => {
		d.hasStarted.then(() => {
			expect(spy.callCount).to.equal(1);
			done();
		});
	});

	// it('Handles config message', (done) => {
	// 	d.hasStarted.then(() => {
	// 		d.handleUnicastMessage({
	// 			type: 'config',
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