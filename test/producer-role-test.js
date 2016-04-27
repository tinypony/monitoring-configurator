import chai from 'chai';
import spies from 'chai-spies';
import mockudp from 'mock-udp';

chai.use(spies);
let expect = chai.expect;
import Daemon from '../src/daemon.js';

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

xdescribe('Producer role', () => {
	let broadcastScope;
	let unicastScope;
	let d;

	beforeEach(() => {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');
		d = new Daemon(producerConf, 12555);
	});

	afterEach(() => {
		d.close();
	});

	it('Broadcasts hello message on start', (done) => {
		d.hasStarted.then(() => {
			expect(broadcastScope.done()).to.be.true;
			console.log('done = true');
			let msg = JSON.parse(broadcastScope.buffer.toString());
			expect(msg.type).to.equal('hello');
			expect(msg.port).to.equal(producerConf.unicast.port);
			done();
		});
	});

	it('Handles config message', () => {
		let spy = chai.spy(d.configureClient);

		d.hasStarted.then(() => {
			d.handleUnicastMessage({
				type: 'config',
				host: '10.0.0.10',
				port: 1337,
				monitoring: {
					host: '10.0.0.10',
					port: 2181
				}
			});

			expect(spy).to.have.been.called.once;
			expect(spy).to.have.been.called.with({
				type: 'config',
				host: '10.0.0.10',
				port: 1337,
				monitoring: {
					host: '10.0.0.10',
					port: 2181
				}
			});
		});
	});

	it('Handles reconfig message', () => {
		let spy = chai.spy(d.configureClient);

		d.hasStarted.then(() => {
			d.handleBroadcastMessage({
				type: 'reconfig',
				host: '10.0.0.10',
				port: 1337,
				monitoring: {
					host: '10.0.0.10',
					port: 2181
				}
			});

			expect(spy).to.have.been.called.once;
			expect(spy).to.have.been.called.with({
				type: 'config',
				host: '10.0.0.10',
				port: 1337,
				monitoring: {
					host: '10.0.0.10',
					port: 2181
				}
			});
		});
	});
});