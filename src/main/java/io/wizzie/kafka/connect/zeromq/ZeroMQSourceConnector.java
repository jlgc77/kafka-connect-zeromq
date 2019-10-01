package io.wizzie.kafka.connect.zeromq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Very simple connector that works with the console. This connector supports
 * both source and sink modes via its 'mode' setting.
 */
public class ZeroMQSourceConnector extends SourceConnector {
	public static final String TOPIC_CONFIG = "topic";
	public static final String SERVER = "server";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(TOPIC_CONFIG, Type.LIST, Importance.HIGH, "The topic to publish data to")
			.define(SERVER, Type.STRING, Importance.HIGH, "The server to Aruba");

	private String topic;
	private String server;

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
		List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
		if (topics.size() != 1) {
			throw new ConfigException(
					"'topic' in ZeroMQSourceConnector configuration requires definition of a single topic");
		}
		topic = topics.get(0);
		server = parsedConfig.getString(SERVER);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ZeroMQSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		Map<String, String> config = new HashMap<>();
		config.put(TOPIC_CONFIG, topic);
		config.put(SERVER, server);
		configs.add(config);
		return configs;
	}

	@Override
	public void stop() {
		// Nothing to do since FileStreamSourceConnector has no background monitoring.
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}
}
