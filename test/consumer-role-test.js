import chai from 'chai';
import spies from 'chai-spies';
import mockudp from 'mock-udp';

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js';

let consumerConf = {
	unicast: {
		port: 12556
	},
	roles: ['consumer'],
	consumers: [{
		topics: ['foo', 'bar'],
		port: 10000
	}],
	monitoring: {
		subnet: '10.0.0.0/16'
	},
	logging: {
		disable: false
	}
};

describe('Consumer role', () => {
	let broadcastScope;
	let unicastScope;
	let d;

	beforeEach(() => {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');
		d = new Daemon(consumerConf, 12555);
	});

	afterEach(() => {
		d.close();
	});

	it('Broadcasts hello message on start', (done) => {
		d.hasStarted.then(() => {
			expect(broadcastScope.done()).to.be.true;
			let msg = JSON.parse(broadcastScope.buffer.toString());
			expect(msg.type).to.equal('hello');
			expect(msg.port).to.equal(consumerConf.unicast.port);
			done();
		});
	});

	it('Handles config message', (done) => {
		d.handleUnicastMessage({
			type: 'config',
			host: '10.0.0.1',
			port: 12556,
			monitoring: {
				host: '10.0.0.1',
				port: 2181
			}
		}).then(() => {
			try {
				expect(unicastScope.done()).to.be.true;
			} catch(e) {
				console.log('Unicast scope was not called');
				expect(false).to.be.true;
				done();
				return;
			}

			let msg = JSON.parse(unicastScope.buffer.toString());
			expect(msg.type).to.equal('subscribe');
			expect(msg.port).to.equal(consumerConf.unicast.port);
			expect(msg.endpoints.length).to.equal(1);
			expect(msg.endpoints[0].topics.length).to.equal(2);
			expect(msg.endpoints[0].port).to.equal(10000);
			done();
		});
	});

	it('Handles reconfig message', (done) => {
		d.handleBroadcastMessage({
			type: 'reconfig',
			host: '10.0.0.1',
			port: 12556,
			monitoring: {
				host: '10.0.0.1',
				port: 2181
			}
		}).then(() => {
			expect(unicastScope.done()).to.be.true;
			let msg = JSON.parse(unicastScope.buffer.toString());
			expect(msg.type).to.equal('subscribe');
			expect(msg.port).to.equal(consumerConf.unicast.port);
			expect(msg.endpoints.length).to.equal(1);
			expect(msg.endpoints[0].topics.length).to.equal(2);
			expect(msg.endpoints[0].port).to.equal(10000);
			done();
		});
	});
});