import chai from 'chai';
import spies from 'chai-spies'
import mockudp from 'mock-udp'
import sinon from 'sinon'
import q from 'q';

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js'
import DatasinkSlave from '../src/roles/datasink-slave-role'
import { MESSAGE_TYPE } from '../src/message-type'
import NODE_TYPE from '../src/node-type'

let datasinkSlaveConf = {
	unicast: {
		port: 12556
	},
	roles: [NODE_TYPE.DATASINK_SLAVE],
	monitoring: {
		subnet: '10.0.0.0/16'
	},
	logging: {
		disable: true
	}
};

describe('Datasink slave role', () => {
	var broadcastScope;
	var unicastScope;
	var d, stub;

	beforeEach(function() {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');

		var defer = q.defer();
		defer.resolve();
		stub = sinon.stub(DatasinkSlave.prototype, 'modifyKafkaConfig').returns(defer.promise);
		d = new Daemon(datasinkSlaveConf, 12555);
	});

	afterEach(function() {
		stub.restore();
		d.close();
	});

	it('Broadcasts hello message on start', done => {
		d.hasStarted.then(() => {
			try {
				expect(broadcastScope.done()).to.be.true;
				let msg = JSON.parse(broadcastScope.buffer.toString());
				expect(msg.type).to.equal(MESSAGE_TYPE.HELLO);
				expect(msg.roles).to.eql([NODE_TYPE.DATASINK_SLAVE]);
				done();
			} catch(e) {
				done(e);
			}
		});
	});

	it('Invokes kafka reconfiguration on config receive', done => {
		d.handleUnicastMessage({
			type: MESSAGE_TYPE.CONFIG,
			uuid: 'lalalalala',
			brokerId: 1,
			host: '10.0.0.1',
			port: 12556,
			monitoring: {
				host: '10.0.0.1',
				port: 2181
			}
		}).then(() => {
			try {
				expect(stub.calledOnce);
				expect(stub.getCall(0).args).to.eql([1, '10.0.0.1', 2181]);
				done();
			} catch(e) {
				done(e);
			}
		});
	});

	it('Sends regslave message after receiving config and kafka reconfiguration', done => {
		d.handleUnicastMessage({
			type: MESSAGE_TYPE.CONFIG,
			uuid: 'lalalalala',
			brokerId: 1,
			host: '10.0.0.1',
			port: 12556,
			monitoring: {
				host: '10.0.0.1',
				port: 2181
			}
		}).then(() => {
			try {
				expect(unicastScope.done()).to.be.true;
				let msg = JSON.parse(unicastScope.buffer.toString());
				expect(msg.brokerId).to.eql(1);
				done();
			} catch(e) {
				done(e);
			}
		});
	});

	it('Broadcasts hello message on reconfigure to start hello-config-regslave sequence', done => {
		d.handleBroadcastMessage({
			type: MESSAGE_TYPE.RECONFIG,
			uuid: 'foobar',
			host: '10.0.0.1',
			port: 12556,
			monitoring: {
				host: '10.0.0.1',
				port: 2181
			}
		}).then(() => {
			try {
				expect(broadcastScope.done()).to.be.true;
				let msg = JSON.parse(broadcastScope.buffer.toString());
				expect(msg.type).to.equal(MESSAGE_TYPE.HELLO);
				expect(msg.roles).to.eql([NODE_TYPE.DATASINK_SLAVE]);
				done();
			} catch(e) {
				done(e);
			}
		});
	});
});

