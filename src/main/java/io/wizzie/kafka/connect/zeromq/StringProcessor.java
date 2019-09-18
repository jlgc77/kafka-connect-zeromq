package io.wizzie.kafka.connect.zeromq;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringProcessor implements ZeroMQMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(StringProcessor.class);
    private ZeroMQMessage zeroMQMessage;
    private String topic;

    @Override
    public StringProcessor process(String topic, ZeroMQMessage message) {
        log.debug("processing data for topic: {}; with message {}", topic, message);
        this.topic = topic;
        zeroMQMessage = message;

        return this;
    }

    @Override
    public SourceRecord[] getRecords() {
        return new SourceRecord[] {
                new SourceRecord(
                        null,
                        null,
                        topic,
                        null,
                        Schema.STRING_SCHEMA,
                        topic,
                        Schema.STRING_SCHEMA,
                        zeroMQMessage.getMessage())};
    }
}