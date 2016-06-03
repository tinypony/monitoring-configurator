import chai from 'chai';
import spies from 'chai-spies';
import sinon from 'sinon'
import mockudp from 'mock-udp';
import q from 'q';

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js';
import Forwarder from '../src/forwarder/forwarder';
import { MESSAGE_TYPE } from '../src/message-type';
import NODE_TYPE from '../src/node-type';

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

describe('Producer role', () => {
	let broadcastScope;
	let unicastScope;
	let d;
	let spy;
	let stub;

	beforeEach(() => {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');
		d = new Daemon(producerConf, 12555);
		spy = sinon.spy(Forwarder.prototype, 'reconfig');

		let defer = q.defer();
		defer.resolve();
		stub = sinon.stub(Forwarder.prototype, 'reconnect').returns(defer.promise);
	});

	afterEach(() => {
		Forwarder.prototype.reconfig.restore();
		Forwarder.prototype.reconnect.restore();
		d.close();
	});

	it('Broadcasts hello message on start', (done) => {
		d.hasStarted.then(() => {
			try {
				expect(broadcastScope.done()).to.be.true;
				let msg = JSON.parse(broadcastScope.buffer.toString());
				expect(msg.type).to.equal(MESSAGE_TYPE.HELLO);
				expect(msg.port).to.equal(producerConf.unicast.port);
				done();
			} catch(e) {
				done(e);
			}
		});
	});

	it('Handles config message', (done) => {
		d.handleUnicastMessage({
			type: MESSAGE_TYPE.CONFIG,
			host: '10.0.0.10',
			port: 1337,
			monitoring: {
				host: '10.0.0.10',
				port: 2181
			}
		}).then(() => {
			try{
				expect(spy.calledOnce).to.be.true;
				expect(stub.calledOnce).to.be.true;
				expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.host', '10.0.0.10');
				expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.port', 2181);
				done();
			} catch(er) {
				done(er);
			}
		});
	});

	it('Handles reconfig message', (done) => {
		d.handleBroadcastMessage({
			type: MESSAGE_TYPE.RECONFIG,
			host: '10.0.0.10',
			port: 1337,
			monitoring: {
				host: '10.0.0.10',
				port: 2181
			}
		}).then(() => {
			try {
				expect(spy.calledOnce).to.be.true;
				expect(stub.calledOnce).to.be.true;
				expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.host', '10.0.0.10');
				expect(spy.getCall(0).args[0]).to.have.deep.property('monitoring.port', 2181);
				done();
			} catch(er) {
				done(er);
			}
		});
	});

	it('Handles cluster resize message', (done) => {
		d.handleBroadcastMessage({
			type: MESSAGE_TYPE.CLUSTER_RESIZE,
			host: '10.0.0.10',
			port: 1337
		}).then(() => {
			try {
				expect(stub.calledOnce).to.be.true;
				done();
			} catch(er) {
				done(er);
			}
		});
	});
});