package io.wizzie.kafka.connect.zeromq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.zeromq.ZMQ;

public class ZeroMQSourceTask extends SourceTask {

	private String topic = null;

	@Override
	public String version() {
		return new ZeroMQSourceConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		topic = props.get(ZeroMQSourceConnector.TOPIC_CONFIG);

	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		List<SourceRecord> records = new ArrayList<>();
		for (int j = 0; j < 10; j++) {
			new Thread(new Runnable() {
				public void run() {
					ZMQ.Context context = ZMQ.context(1);
					ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
					subscriber.connect("tcp://127.0.0.1:5555");
					subscriber.subscribe("toutiao".getBytes());

					try {
						while (true) {
							byte[] message = subscriber.recv();
							records.add(new SourceRecord(null, null, topic, null, Schema.STRING_SCHEMA, topic,
									Schema.STRING_SCHEMA, new String(message)));
						}
					} finally {
						subscriber.close();
						context.term();
					}
				}
			}).start();
		}
		return records;
	}

	@Override
	public void stop() {
	}
}
