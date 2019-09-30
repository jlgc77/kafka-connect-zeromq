package io.wizzie.kafka.connect.zeromq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.wizzie.kafka.connect.zeromq.model.Schema.location;
import io.wizzie.kafka.connect.zeromq.model.Schema.nb_event;
import io.wizzie.kafka.connect.zeromq.model.Schema.presence;
import io.wizzie.kafka.connect.zeromq.model.Schema.proximity;

public class ZeroMQSourceTask2 extends SourceTask {

	private static Logger log = LoggerFactory.getLogger(ZeroMQSourceTask2.class);

	private String topic = null;
	private ZMQ.Socket socket = null;
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
//		Context context = ZMQ.context(1);
//		Socket socket = context.socket(ZMQ.SUB);
//		socket.connect("tcp://192.168.223.43:7779");
//		socket.subscribe("".getBytes());

		byte[] currByte = null;
		String typeEvent = null;
		nb_event event = null;
		String tipo = null;

		while (!Thread.currentThread().isInterrupted()) {
			typeEvent = socket.recvStr(0);
			currByte = socket.recv(0);
			System.out.println("Event Type >>> " + typeEvent);

			while (socket.hasReceiveMore()) {
				System.out.println("Entro en while del socker");
				byte[] moreBytes = socket.recv(0);
				currByte = Bytes.concat(currByte, moreBytes);
			}

			try {
				event = nb_event.parseFrom(currByte);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			if (event.getPresence() != null) {
//				System.out.println("es presence");
//				presence presence = event.getPresence();
//				System.out.println("es presence to strig  >> " + presence.toString());
				tipo = "presence";
			} else if (event.getProximity() != null) {
//				System.out.println("es proximity");
//				proximity proximity = event.getProximity();
//				System.out.println("es proximity to strig  >> " + proximity.toString());
				tipo = "proximity";
			} else if (event.getLocation() != null) {
//				System.out.println("es location");
//				location location = event.getLocation();
//				System.out.println("latitude >> " + location.getLatitude());
//				System.out.println("longitude >>" + location.getLongitude());
				tipo = "location";
			} else {
//				System.out.println("es otro");
				tipo = "otro";
			}
			try {
				System.out.println("Mando al topic el tipo >>> " + tipo);
				wQueue.put(zeroMQSourceConnectorConfig
						.getConfiguredInstance("message_processor_class", ZeroMQMessageProcessor.class)
						.process(topic, new ZeroMQMessage(topic, tipo)));
				System.out.println("Fin >>> " + tipo);
			} catch (InterruptedException e) {
				e.printStackTrace();
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

		context = ZMQ.context(1);
		socket = context.socket(ZMQ.SUB);
		socket.connect("tcp://192.168.223.43:7779");
		socket.subscribe("".getBytes());

//		subscriber.connect("tcp://192.168.1.141:5555");
//		subscriber.subscribe("toutiao".getBytes());

		startSubscriber();
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		List<SourceRecord> records = new ArrayList<>();
		System.out.println("Polling data >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		ZeroMQMessageProcessor zeroMQMessageProcessor = wQueue.take();
		Collections.addAll(records, zeroMQMessageProcessor.getRecords());
		return records;
	}

	@Override
	public void stop() {
		isRunning = false;
		socket.close();
		context.term();
	}
}
