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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.wizzie.kafka.connect.zeromq.data.LocationData;
import io.wizzie.kafka.connect.zeromq.model.Schema.location;
import io.wizzie.kafka.connect.zeromq.model.Schema.nb_event;

public class ZeroMQSourceTask extends SourceTask {

	private static Logger log = LoggerFactory.getLogger(ZeroMQSourceTask.class);
	private final static String LOCATION = "location";

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
		new Thread(new Runnable() {
			public void run() {
				byte[] currByte = null;
				String typeEvent = null;
				nb_event event = null;
				String tipo = null;
				while (isRunning) {
					typeEvent = subscriber.recvStr(0);
					currByte = subscriber.recv(0);
					LocationData locationData = new LocationData();
					while (subscriber.hasReceiveMore()) {
						byte[] moreBytes = subscriber.recv(0);
						currByte = Bytes.concat(currByte, moreBytes);
					}

					try {
						event = nb_event.parseFrom(currByte);
					} catch (InvalidProtocolBufferException e) {
						e.printStackTrace();
					}
					if (typeEvent.equalsIgnoreCase(LOCATION)) {
						tipo = LOCATION;
						location location = event.getLocation();
						locationData.setX(location.getStaLocationX());
						locationData.setY(location.getStaLocationY());
						try {
							ObjectMapper objectMapper = new ObjectMapper();
							String json = objectMapper.writeValueAsString(locationData);
							wQueue.put(zeroMQSourceConnectorConfig
									.getConfiguredInstance("message_processor_class", ZeroMQMessageProcessor.class)
									.process(topic, new ZeroMQMessage(topic, json)));
							System.out.println("Fin >>> " + tipo);
						} catch (InterruptedException | JsonProcessingException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}).start();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("Starting ZeroMQ connector");
		zeroMQSourceConnectorConfig = new ZeroMQSourceConnectorConfig(props);
		topic = props.get(ZeroMQSourceConnector.TOPIC_CONFIG);
		context = ZMQ.context(1);
		subscriber = context.socket(ZMQ.SUB);
		subscriber.connect(props.get(ZeroMQSourceConnector.SERVER));
		subscriber.subscribe(LOCATION.getBytes());
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

	public static String byteStringToStringForMac(ByteString byteStr) {
		String result = "";
		for (int i = 0; i < byteStr.size(); ++i) {
			if (i != 0)
				result += ":";
			result += String.format("%02X", byteStr.byteAt(i));
		}
		return result;
	}
}
