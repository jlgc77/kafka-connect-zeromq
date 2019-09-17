package io.wizzie.kafka.connect.zeromq;

import org.zeromq.ZMQ;

public class Subscriber {

	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_YELLOW = "\u001B[33m";

	public static void main(String args[]) {

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
							System.out.println(ANSI_YELLOW + Thread.currentThread().getName() + " receive : "
									+ new String(message) + ANSI_RESET);
						}
					} finally {
						subscriber.close();
						context.term();
					}
				}
			}).start();
		}
	}
}