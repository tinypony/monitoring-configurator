import Consumer from '../src/roles/p2p-consumer-role';
import { MESSAGE_TYPE } from '../src/message-type';
import NODE_TYPE from '../src/node-type';
import { expect } from 'chai';
import sinon from 'sinon';
import q from 'q';

describe('p2p Consumer', () => {
	const consumerConf = {
		unicast: {
			port: 12556
		},
		roles: [NODE_TYPE.P2PCONSUMER],
		consumers: [{
			topics: ['foo'],
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

	let consumer, spy;

	beforeEach(() => {
		const sockets = {
			unicast: {},
			broadcast: {}
		};

		var defer = q.defer();
		defer.resolve();
		spy = sinon.stub(Consumer.prototype, 'respondTo').returns(defer.promise);
		consumer = new Consumer(null, consumerConf, sockets);
	});

	afterEach(() => {
		spy.restore();
	});

	it('Handles reconfig message from tracker by responding with subscribe', done => {
		consumer.handleTReconfig(trackerReconfig).done(() => {
			expect(spy.calledOnce).to.be.true;
			expect(spy.getCall(0).args).to.eql([
				trackerReconfig, 
				consumer.getSubscribeMessage()
			]);
			done();
		});
	});
});
