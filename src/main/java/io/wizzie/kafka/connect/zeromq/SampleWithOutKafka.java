package io.wizzie.kafka.connect.zeromq;

import java.io.IOException;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;

import io.wizzie.kafka.connect.zeromq.data.LocationData;
import io.wizzie.kafka.connect.zeromq.model.Schema.location;
import io.wizzie.kafka.connect.zeromq.model.Schema.nb_event;

public class SampleWithOutKafka {

	public static void main(String[] args) throws IOException {
		Context context = ZMQ.context(1);

		Socket socket = context.socket(ZMQ.SUB);
		socket.connect("tcp://192.168.223.43:7779");
		// subscribe to all available topics
		socket.subscribe("".getBytes());
//		socket.subscribe("proximity".getBytes());
//		socket.subscribe("presence".getBytes());

		byte[] currByte = null;
		String currentAddress = null;

		while (!Thread.currentThread().isInterrupted()) {
			currentAddress = socket.recvStr(0);
			currByte = socket.recv(0);
			System.out.println("CurrentAdress es >>> " + currentAddress);
			LocationData locationData = new LocationData();

			while (socket.hasReceiveMore()) {
				byte[] moreBytes = socket.recv(0);
				currByte = Bytes.concat(currByte, moreBytes);
			}

			nb_event evento = nb_event.parseFrom(currByte);
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
			objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
//			objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			String json = objectMapper.writeValueAsString(evento);
			System.out.println(json);

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