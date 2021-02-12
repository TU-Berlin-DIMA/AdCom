package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.fusesource.mqtt.client.*;

import java.util.Date;

public class MqttDataSource extends RichSourceFunction<String> {
	private static final String DEFAUL_HOST = "127.0.0.1";
	private static final int DEFAUL_PORT = 1883;
	private static final String SHUTDOWN_SIGNAL = "SHUTDOWN";
	private String host;
	private String topic;
	private int port;
	private QoS qos;
	private boolean running;
	private boolean collectWithTimestamp;

	public MqttDataSource(String topic) throws Exception {
		this(topic, DEFAUL_HOST, DEFAUL_PORT, false);
	}

	public MqttDataSource(String topic, String host, int port) throws Exception {
		this(topic, host, port, false);
	}

	public MqttDataSource(String topic, String host, int port, boolean collectWithTimestamp) throws Exception {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = QoS.AT_LEAST_ONCE;
		this.running = true;
		this.collectWithTimestamp = collectWithTimestamp;
		this.disclaimer();
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		MQTT mqtt = new MQTT();
		mqtt.setHost(host, port);
		BlockingConnection blockingConnection = mqtt.blockingConnection();
		blockingConnection.connect();

		byte[] qoses = blockingConnection.subscribe(new Topic[]{new Topic(topic, qos)});

		while (running && blockingConnection.isConnected()) {
			Message message = blockingConnection.receive();
			String payload = new String(message.getPayload());
			message.ack();
			if (SHUTDOWN_SIGNAL.equals(payload)) {
				this.cancel();
			} else {
				if (collectWithTimestamp) {
					ctx.collectWithTimestamp(payload, new Date().getTime());
					// ctx.emitWatermark(new Watermark(eventTime.getTime()));
				} else {
					ctx.collect(payload);
				}
			}
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}

	private void disclaimer() {
		System.out.println("Use the following command to consume data from the application >>>");
		System.out.println("mosquitto_sub -h " + host + " -p " + port + " -t " + topic);
		System.out.println();
	}
}
