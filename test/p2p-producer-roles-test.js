import Producer from '../src/roles/p2p-producer-role';
import { MESSAGE_TYPE } from '../src/message-type';
import NODE_TYPE from '../src/node-type';
import { expect } from 'chai';
import sinon from 'sinon';
import q from 'q';

describe('p2p Producer', () => {
	const producerConf = {
		unicast: {
			port: 12556
		},
		roles: [NODE_TYPE.P2PPRODUCER],
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

	const trackerReconfig = {
		type: MESSAGE_TYPE.TRECONFIG,
		host: '10.0.0.10',
		port: 1234
	};

	let producer, spy;

	beforeEach(() => {
		const sockets = {
			unicast: {},
			broadcast: {}
		};

		var defer = q.defer();
		defer.resolve();
		spy = sinon.stub(Producer.prototype, 'respondTo').returns(defer.promise);
		producer = new Producer(null, producerConf, sockets);
	});

	afterEach(() => {
		spy.restore();
	});

	it('Handles reconfig message from tracker by responding with publish', done => {
		producer.handleTReconfig(trackerReconfig).done(() => {
			expect(spy.calledOnce).to.be.true;
			expect(spy.getCall(0).args).to.eql([
				trackerReconfig, 
				producer.getPublishMessage()
			]);
			done();
		});
	});
});
