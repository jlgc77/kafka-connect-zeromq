package io.wizzie.kafka.connect.zeromq;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.wizzie.kafka.connect.zeromq.model.Schema.location;
import io.wizzie.kafka.connect.zeromq.model.Schema.nb_event;
import io.wizzie.kafka.connect.zeromq.model.Schema.presence;
import io.wizzie.kafka.connect.zeromq.model.Schema.proximity;

public class ZeroMQSourceTask extends SourceTask {

	private static Logger log = LoggerFactory.getLogger(ZeroMQSourceTask.class);

	private String topic = null;
	private ZMQ.Socket subscriber = null;
	private ZMQ.Context context = null;
	private BlockingQueue<ZeroMQMessageProcessor> wQueue = new LinkedBlockingQueue<>();
	private boolean isRunning = true;

	private ZeroMQSourceConnectorConfig zeroMQSourceConnectorConfig = null;

	@Override
	public String version() {
		return new ZeroMQSourceConnector().version();
	}

	private void startSubscriber() {
		log.info("Starting subscriber");
		Context context = ZMQ.context(1);

		Socket socket = context.socket(ZMQ.SUB);
		socket.connect("tcp://192.168.223.43:7779");
		// subscribe to all available topics
		socket.subscribe("".getBytes());
//		socket.subscribe("proximity".getBytes());
//		socket.subscribe("presence".getBytes());

		byte[] currByte = null;
		String currentAddress = null;
		nb_event evento = null;

		while (!Thread.currentThread().isInterrupted()) {
			currentAddress = socket.recvStr(0);
			currByte = socket.recv(0);
			// System.out.println("CurrentAdress es >>> " + currentAddress);

			while (socket.hasReceiveMore()) {
				System.out.println("Entro en while del socker");
				byte[] moreBytes = socket.recv(0);
				currByte = Bytes.concat(currByte, moreBytes);
			}

			try {
				evento = nb_event.parseFrom(currByte);
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (evento.getPresence() != null) {
				System.out.println("es presence");
				presence presence = evento.getPresence();
				System.out.println("es presence to strig  >> " + presence.toString());
			} else if (evento.getProximity() != null) {
				System.out.println("es proximity");
				proximity proximity = evento.getProximity();
				System.out.println("es proximity to strig  >> " + proximity.toString());
			} else if (evento.getLocation() != null) {
				System.out.println("es location");
				location location = evento.getLocation();
				System.out.println("latitude >> " + location.getLatitude());
				System.out.println("longitude >>" + location.getLongitude());
			} else {
				System.out.println("es otro");
			}
		}

	}

	public static String byteStringToStringForMac(ByteString byteStr) {
		String result = "";
		for (int i = 0; i < byteStr.size(); ++i) {
			if (i != 0)
				result += ":";
			result += String.format("%02X", byteStr.byteAt(i));
		}
		return result;
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("Starting ZeroMQ connector");

		zeroMQSourceConnectorConfig = new ZeroMQSourceConnectorConfig(props);

		topic = props.get(ZeroMQSourceConnector.TOPIC_CONFIG);

//		context = ZMQ.context(1);

//		subscriber = context.socket(ZMQ.SUB);
//		subscriber.connect("tcp://192.168.1.141:5555");
//		subscriber.subscribe("toutiao".getBytes());

//		subscriber.connect("tcp://192.168.223.43:7779");
//		subscriber.subscribe("proximity".getBytes());

		startSubscriber();
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		List<SourceRecord> records = new ArrayList<>();
		log.info("Polling data");
		ZeroMQMessageProcessor zeroMQMessageProcessor = wQueue.take();
		Collections.addAll(records, zeroMQMessageProcessor.getRecords());
		return records;
	}

	@Override
	public void stop() {
		isRunning = false;
		subscriber.close();
		context.term();
	}
}
