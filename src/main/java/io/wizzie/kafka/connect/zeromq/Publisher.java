package io.wizzie.kafka.connect.zeromq;

import org.zeromq.ZMQ;

public class Publisher {

	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_BLUE = "\u001B[34m";

	public static void main(String args[]) {
		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket publisher = context.socket(ZMQ.PUB);
		publisher.bind("tcp://*:5555");

		while (!Thread.currentThread().isInterrupted()) {
			String message = "toutiao hello";
			publisher.send(message.getBytes());
			System.out.println(ANSI_BLUE + "sent : " + message + ANSI_RESET);
		}

		publisher.close();
		context.term();
	}
}