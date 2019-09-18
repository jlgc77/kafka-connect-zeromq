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
		new Thread(new Runnable() {
			public void run() {
				while (isRunning) {
					
					byte[] message = subscriber.recv();
					log.info("Received new message!");
					try {
						
						wQueue.put(
								zeroMQSourceConnectorConfig.getConfiguredInstance("message_processor_class", ZeroMQMessageProcessor.class)
										.process(topic, new ZeroMQMessage(topic, new String(message))));
					} catch (InterruptedException e) {
						e.printStackTrace();
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
		subscriber.connect("tcp://192.168.1.111:5555");
		subscriber.subscribe("toutiao".getBytes());
		
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
