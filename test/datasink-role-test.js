import chai from 'chai';
import spies from 'chai-spies';
import mockudp from 'mock-udp';
import DatasinkRole from '../src/roles/datasink-role';
import _ from 'underscore';

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
		disable: true
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

	it('Broadcasts reconfigure message on start', done => {
		d.hasStarted.then(() => {
			try {
				expect(broadcastScope.done()).to.be.true;
				let msg = JSON.parse(broadcastScope.buffer.toString());
				expect(msg.type).to.equal('reconfig');
				done();
			} catch(e) {
				done(e);
			}
		});
	});

	it('Sends unicast response to hello message', done => {
		d.handleBroadcastMessage({
			type: 'hello',
			uuid: 'lalalalala',
			roles: ['producer'],
			host: '10.0.0.1',
			port: 12556
		}).then(() => {
			try {
				expect(unicastScope.done()).to.be.true;
				let msg = JSON.parse(unicastScope.buffer.toString());
				expect(msg.type).to.eql('config');
				done();
			} catch(e) {
				done(e);
			}
		});
	});

	it('Sends next unique broker id to a new slave', done => {
		d.handleBroadcastMessage({
			type: 'hello',
			uuid: 'hop-la-lai-la',
			roles: ['producer', 'datasink-slave'],
			host: '10.0.0.1',
			port: 12556
		}).then(() => {
			try {
				let msg = JSON.parse(unicastScope.buffer.toString());
				expect(msg.brokerId).to.eql(1);
				done();
			} catch(e) {
				done(e);
			}
		})
	});

	it('updates broker list upon receiving slave register message', done => {
		d.handleUnicastMessage({
			type: 'regslave',
			roles: ['datasink-slave', 'producer'],
			host: '10.0.0.2',
			port: 12566,
			brokerId: 42
		}).then(() => {
			try {
				let ds = _.find( d.roles, r => r instanceof DatasinkRole );
				expect(ds.brokers).to.eql([0, 42]);
				done();
			} catch(er) {
				done(er);
			}
		});
	});
});

