package io.wizzie.kafka.connect.zeromq;

import org.apache.kafka.connect.source.SourceRecord;

public interface ZeroMQMessageProcessor {
    ZeroMQMessageProcessor process(String topic, ZeroMQMessage message);

    SourceRecord[] getRecords();
}
