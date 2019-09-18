package io.wizzie.kafka.connect.zeromq;

public class ZeroMQMessage {
	private String topic, message;

	public ZeroMQMessage(String topic, String message) {
		this.topic = topic;
		this.message = message;
	}

	public String getTopic() {
		return topic;
	}

	public String getMessage() {
		return message;
	}
}
