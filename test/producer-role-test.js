import chai from 'chai';
import spies from 'chai-spies';
import sinon from 'sinon'
import mockudp from 'mock-udp';

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js';
import Forwarder from '../src/forwarder/forwarder';

let producerConf = {
	unicast: {
		port: 12556
	},
	roles: ['producer'],
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

describe('Producer role', () => {
	let broadcastScope;
	let unicastScope;
	let d;
	let spy;

	beforeEach(() => {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');
		d = new Daemon(producerConf, 12555);
		spy = sinon.spy(Forwarder.prototype, 'reconfig');
	});

	afterEach(() => {
		Forwarder.prototype.reconfig.restore();
		d.close();
	});

	it('Broadcasts hello message on start', (done) => {
		d.hasStarted.then(() => {
			expect(broadcastScope.done()).to.be.true;
			let msg = JSON.parse(broadcastScope.buffer.toString());
			expect(msg.type).to.equal('hello');
			expect(msg.port).to.equal(producerConf.unicast.port);
			done();
		});
	});

	it('Handles config message', (done) => {
		d.handleUnicastMessage({
			type: 'config',
			host: '10.0.0.10',
			port: 1337,
			monitoring: {
				host: '10.0.0.10',
				port: 2181
			}
		}).then(()=>{
			expect(spy.calledOnce).to.be.true;
			expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.host', '10.0.0.10');
			expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.port', 2181);

			done();
		});
	});

	it('Handles reconfig message', (done) => {
		d.handleBroadcastMessage({
			type: 'reconfig',
			host: '10.0.0.10',
			port: 1337,
			monitoring: {
				host: '10.0.0.10',
				port: 2181
			}
		}).then(() => {
			expect(spy.calledOnce).to.be.true;
			expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.host', '10.0.0.10');
			expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.port', 2181);
			done();
		});
	});
});