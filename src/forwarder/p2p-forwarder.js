import dgram from 'dgram';
import net from 'net';
import _ from 'underscore';
import q from 'q';
import {Client, HighLevelProducer} from 'kafka-node';
import uuid from 'node-uuid';
import winston from 'winston';
import Dequeue from 'double-ended-queue';

function isValidPort(port) {
	return _.isNumber(port) && port > 0 && port < 65535;
}


class P2PForwarder {
	constructor(config) {
		this.id = uuid.v4();
		this.config = config;
		this.out = dgram.createSocket('udp4');

		this.logger = new winston.Logger({
			transports: [new winston.transports.Console()]
		});

		if( config.logging && config.logging.disable ) {
			this.logger.remove(winston.transports.Console);
		}


		/**
		 * fwd.port,
		 * fwd.topic,
		 * fwd.protocol //udp or tcp
		 */
		this.forward_map = {};
		this.forward_ports = [];
		_.each(config.producers, fwd => {
			if(fwd.protocol === 'tcp')
				this.createTcpSocket(fwd).done(binding => { this.forward_ports.push(binding) });
			else
				this.createUDPSocket(fwd).done(binding => { this.forward_ports.push(binding) });
		});
	}

	createUDPSocket(fwd) {
		var defer = q.defer();
		this.logger.info("[p2p-Forwarder] Forwarding configuration = %d => %s", fwd.port, fwd.topic);
		var skt = dgram.createSocket('udp4');
		skt.bind(fwd.port, '127.0.0.1');

		skt.on('error', er => {
			this.logger.warn(`[Forwarder.constructor()] ${er}`);
		});

		var binding = {
			protocol: 'udp',
			socket: skt,
			port: fwd.port,
			topic: fwd.topic
		};

		skt.on("message", this.forward.bind(this, fwd.topic));

		defer.resolve(binding);
		return defer.promise;
	}

	createTcpSocket(fwd) {
		var defer = q.defer();
		var FIFO = new Dequeue();
		// Start a TCP Server
		net.createServer(socket => {
			var binding = {
				protocol: 'tcp',
				port: fwd.port,
				topic: fwd.topic,
				clients: []
			};
			
			// Identify this client
			socket.name = socket.remoteAddress + ":" + socket.remotePort;

			// Put this new client in the list
			binding.clients.push(socket);

			// Handle incoming messages from clients.
			socket.on('data', this.forward.bind(this, fwd.topic));
			// Remove the client from the list when it leaves
			socket.on('end', () => {
				binding.clients.splice(binding.clients.indexOf(socket), 1);
			});

			defer.resolve(binding);
		}).listen(fwd.port);

		return defer.promise;
	}

	addForwardingInfo(topic, dest) {
		let destinations = this.forward_map[topic];
		
		if(!destinations) {
			destinations = [];
		}

		destinations.push(dest);
		this.forward_map[topic] = destinations;
	}

	send(host, port, message) {
		var defer = q.defer();
		this.out.send(
			new Buffer(message),
			0,
			message.length,
			port,
			host,
			err => {
				if(err) { return defer.reject(err); } 
				else { return defer.resolve(); }
			}
		);

		return defer.promise;
	}

	forward(topic, data) {
		var msgStr = data.toString();
		
		_.each(this.forward_map[topic], endpoint => {
			this.send(endpoint.host, endpoint.port, msgStr);
		});
	}
}

export default P2PForwarder;
