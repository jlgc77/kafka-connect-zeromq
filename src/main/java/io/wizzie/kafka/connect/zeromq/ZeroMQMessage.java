package io.wizzie.kafka.connect.zeromq;

public class ZeroMQMessage {
	private String topic;
	private Object message;

	public ZeroMQMessage(String topic, Object message) {
		this.topic = topic;
		this.message = message;
	}

	public String getTopic() {
		return topic;
	}

	public Object getMessage() {
		return message;
	}
}
