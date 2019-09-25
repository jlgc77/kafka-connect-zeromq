package io.wizzie.kafka.connect.zeromq;

import java.io.IOException;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;

import io.wizzie.kafka.connect.zeromq.model.Schema.nb_event;
import io.wizzie.kafka.connect.zeromq.model.Schema.proximity;

public class SampleAleFeedReader {

	public static void main(String[] args) throws IOException {
		Context context = ZMQ.context(1);

		Socket socket = context.socket(ZMQ.SUB);
		socket.connect("tcp://192.168.223.43:7779");
		// subscribe to all available topics
//		socket.subscribe("".getBytes());
		socket.subscribe("proximity".getBytes());
//		socket.subscribe("presence".getBytes());

		byte[] currByte = null;
		String currentAddress = null;

		while (!Thread.currentThread().isInterrupted()) {
			currentAddress = socket.recvStr(0);
			currByte = socket.recv(0);

			while (socket.hasReceiveMore()) {
				System.out.println("multi-part zmq message ");
				byte[] moreBytes = socket.recv(0);
				currByte = Bytes.concat(currByte, moreBytes);
			}

			nb_event evento = nb_event.parseFrom(currByte);
//			System.out.println("EVENTO >>> ");
//			System.out.println(evento);

			/**
			 * Datos proximity
			 */
			proximity item = proximity.parseFrom(currByte);
			System.out.println("Entidad >>> ");
//			System.out.println(item);
			System.out.println("Mac address client >>> ");
			System.out.println(item.getStaEthMac());
			System.out.println(item.getHashedStaEthMac());
			String sta_eth_mac = byteStringToStringForMac(item.getHashedStaEthMac());
			System.out.println(sta_eth_mac);
			System.out.println("Fin mac >>> ");
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
}