import chai from 'chai';
import spies from 'chai-spies'
import mockudp from 'mock-udp'
import sinon from 'sinon'

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js'
import DatasinkSlave from '../src/roles/datasink-slave-role'

let datasinkSlaveConf = {
	unicast: {
		port: 12556
	},
	roles: ['datasink-slave'],
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
	var d;
	var spy;

	beforeEach(function() {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');
		spy = sinon.spy(DatasinkSlave.prototype, 'modifyKafkaConfig');
		d = new Daemon(datasinkSlaveConf, 12555);
	});

	afterEach(function() {
		d.close();
		DatasinkSlave.prototype.modifyKafkaConfig.restore();
	});

	it('Broadcasts hello message on start', (done) => {
		d.hasStarted.then(() => {
			try {
				expect(broadcastScope.done()).to.be.true;
			} catch(e) {
				console.log('broadcastScope was not called');
				expect(false).to.be.true;
				done();
			}
			let msg = JSON.parse(broadcastScope.buffer.toString());
			expect(msg.type).to.equal('hello');
			done();
		});
	});

	it('Invokes kafka reconfiguration on config receive', (done) => {
		d.handleUnicastMessage({
			type: 'config',
			uuid: 'lalalalala',
			host: '10.0.0.1',
			port: 12556,
			monitoring: {
				host: '10.0.0.1',
				port: 2181
			}
		}).then(() => {
			expect(spy.calledOnce).to.be.true;
			expect(spy.getCall(0).args[0]).to.eql('10.0.0.1');
			expect(spy.getCall(0).args[1]).to.eql(2181);
			done();
		});
	});
});

