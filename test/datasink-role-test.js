import chai from 'chai';
import spies from 'chai-spies';
import mockudp from 'mock-udp';

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js';


let datasinkConf = {
	unicast: {
		port: 12556
	},
	roles: ['datasink'],
	monitoring: {
		subnet: '10.0.0.0/16',
		port: 2181
	},
	logging: {
		disable: false
	}
};

describe('Datasink role', () => {
	let broadcastScope;
	let unicastScope;
	let d;

	beforeEach(() => {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');
		d = new Daemon(datasinkConf, 12555);
	});

	afterEach(() => {
		d.close();
	});

	it('Broadcasts reconfigure message on start', (done) => {
		d.hasStarted.then(() => {
			try {
				expect(broadcastScope.done()).to.be.true;
			} catch(e) {
				console.log('broadcastScope was not called');
				expect(false).to.be.true;
				done();
			}
			let msg = JSON.parse(broadcastScope.buffer.toString());
			expect(msg.type).to.equal('reconfig');
			done();
		});
	});

	it('Sends unicast response to hello message', (done) => {
		d.handleBroadcastMessage({
			type: 'hello',
			uuid: 'lalalalala',
			host: '10.0.0.1',
			port: 12556
		}).then(() => {
			try {
				expect(unicastScope.done()).to.be.true;
			} catch(e) {
				console.log('broadcastScope was not called');
				expect(false).to.be.true;
				done();
			}
			let msg = JSON.parse(unicastScope.buffer.toString());
			expect(msg.type).to.equal('config');
			done();
		});
	});
});

