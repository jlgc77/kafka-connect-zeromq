package io.wizzie.kafka.connect.zeromq;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class ZeroMQSourceTask extends SourceTask {

	@Override
	public String version() {
		return new ZeroMQSourceConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {

	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		return null;
	}

	@Override
	public void stop() {
	}
}
