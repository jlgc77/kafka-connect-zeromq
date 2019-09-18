package io.wizzie.kafka.connect.zeromq;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ZeroMQSourceConnectorConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(ZeroMQSourceConnectorConfig.class);
    static ConfigDef config = baseConfigDef();

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define("message_processor_class", ConfigDef.Type.CLASS, StringProcessor.class, ConfigDef.Importance.HIGH,
                        "message processor to use");
    }

    public ZeroMQSourceConnectorConfig(Map<String, String> props) {
        super(config, props);
        log.info("Initialize transform process properties");
    }
}