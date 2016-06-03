import chai from 'chai';
import spies from 'chai-spies';
import mockudp from 'mock-udp';
import DatasinkRole from '../src/roles/datasink-role';
import _ from 'underscore';

chai.use(spies);
let expect = chai.expect;
import q from 'q';
import sinon from 'sinon';
import Daemon from '../src/daemon.js';
import { MESSAGE_TYPE } from '../src/message-type';
import NODE_TYPE from '../src/node-type';


let datasinkConf = {
	unicast: {
		port: 12556
	},
	roles: [NODE_TYPE.DATASINK],
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
	let stub;
	let stubBroadcast;
	let d;

	beforeEach(() => {
		broadcastScope  = mockudp('10.0.255.255:12555');
		unicastScope = mockudp('10.0.0.1:12556');
		let defer = q.defer();
		defer.resolve();
		stub = sinon.stub(DatasinkRole.prototype, 'rebalanceCluster').returns(defer.promise);
		stubBroadcast = sinon.stub(DatasinkRole.prototype, 'broadcast').returns(defer.promise);
		d = new Daemon(datasinkConf, 12555);
	});

	afterEach(() => {
		DatasinkRole.prototype.rebalanceCluster.restore();
		DatasinkRole.prototype.broadcast.restore();
		d.close();
	});

	it('Broadcasts reconfigure message on start', done => {
		d.hasStarted.then(() => {
			try {
				expect(stubBroadcast.calledOnce).to.be.true;
				let msg = JSON.parse(stubBroadcast.getCall(0).args[0]);
				expect(msg.type).to.equal(MESSAGE_TYPE.RECONFIG);
				done();
			} catch(e) {
				done(e);
			}
		});
	});

	it('Sends unicast response to hello message', done => {
		d.handleBroadcastMessage({
			type: MESSAGE_TYPE.HELLO,
			uuid: 'lalalalala',
			roles: [NODE_TYPE.PRODUCER],
			host: '10.0.0.1',
			port: 12556
		}).then(() => {
			try {
				expect(unicastScope.done()).to.be.true;
				let msg = JSON.parse(unicastScope.buffer.toString());
				expect(msg.type).to.eql(MESSAGE_TYPE.CONFIG);
				done();
			} catch(e) {
				done(e);
			}
		});
	});

	it('Sends next unique broker id to a new slave', done => {
		d.handleBroadcastMessage({
			type: MESSAGE_TYPE.HELLO,
			uuid: 'hop-la-lai-la',
			roles: [NODE_TYPE.PRODUCER, NODE_TYPE.DATASINK_SLAVE],
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
			type: MESSAGE_TYPE.REGISTER_SLAVE,
			roles: [NODE_TYPE.DATASINK_SLAVE, NODE_TYPE.PRODUCER],
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

	it('broadcasts cluster resize message message after handling slave registration', done => {

		d.handleUnicastMessage({
			type: MESSAGE_TYPE.REGISTER_SLAVE,
			roles: [NODE_TYPE.DATASINK_SLAVE, NODE_TYPE.PRODUCER],
			host: '10.0.0.2',
			port: 12566,
			brokerId: 42
		}).then(() => {
			try {
				expect(stubBroadcast.calledOnce);
				let clusterResize = JSON.parse(stubBroadcast.getCall(0).args[0]);
				expect(clusterResize.type === MESSAGE_TYPE.CLUSTER_RESIZE);
				done();
			} catch(er) {
				done(er);
			}
		});
	});

});

