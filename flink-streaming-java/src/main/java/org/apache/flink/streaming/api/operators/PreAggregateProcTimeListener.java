package org.apache.flink.streaming.api.operators;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import org.fusesource.mqtt.client.*;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This tread is created by each pre-agg operator to receive new parameters of processing timeout.
 * It receives parameter from the controller in the JobManager.
 * <pre>
 *      Changes the frequency that the pre-aggregate emits batches of data:
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "1000"
 * </pre>
 */
public class PreAggregateProcTimeListener extends Thread implements Serializable {

	public static final String TOPIC_PRE_AGGREGATE_PARAMETER = "topic-pre-aggregate-parameter";
	private final int MIN_INTERVAL_MS = 50;
	private final String topic;
	private final String host;
	private final int port;
	private final int subtaskId;
	private final boolean enableController;
	private BlockingConnection subscriber;
	private MQTT mqtt;
	private boolean running = false;
	private long intervalMs;

	public PreAggregateProcTimeListener(long intervalMs, int subtaskId) {
		// Job manager and taskManager have to be deployed on the same machine, otherwise use the other constructor
		this("127.0.0.1", intervalMs, subtaskId, false);
	}

	public PreAggregateProcTimeListener(
		String host,
		long intervalMs,
		int subtaskId,
		boolean enableController) {
		if (Strings.isNullOrEmpty(host) || host.equalsIgnoreCase("localhost")) {
			this.host = "127.0.0.1";
		} else {
			this.host = host;
		}
		this.port = 1883;
		this.intervalMs = intervalMs;
		this.subtaskId = subtaskId;
		this.topic = TOPIC_PRE_AGGREGATE_PARAMETER;
		this.enableController = enableController;
		this.running = true;
		this.disclaimer();
	}

	private void connect() throws Exception {
		this.mqtt = new MQTT();
		this.mqtt.setHost(host, port);
		this.subscriber = mqtt.blockingConnection();
		this.subscriber.connect();
		Topic[] topics = new Topic[]{new Topic(this.topic, QoS.AT_LEAST_ONCE)};
		this.subscriber.subscribe(topics);
	}

	public void run() {
		if (!this.enableController) {
			System.out.println(
				"[PreAggregateProcTimeListener] controller is not enabled then the listener doesn't have to start.");
		} else {
			while (running) {
				try {
					if (subscriber == null) {
						connect();
					}
					Message msg = subscriber.receive(10, TimeUnit.SECONDS);
					if (msg != null) {
						msg.ack();
						String message = new String(msg.getPayload(), UTF_8);
						System.out.println(
							"[PreAggregateProcTimeListener] pre-agg[" + subtaskId
								+ "] received msg: "
								+ message);
						if (isInteger(message)) {
							long newIntervalMs = Long.valueOf(message).longValue();
							// Not allow to have intervals less than 50 milliseconds
							if (newIntervalMs >= MIN_INTERVAL_MS) {
								this.intervalMs = newIntervalMs;
							} else {
								this.intervalMs = MIN_INTERVAL_MS;
								System.out.println(
									"[PreAggregateProcTimeListener] WARN: Interval less than "
										+ MIN_INTERVAL_MS + " milliseconds are set to "
										+ MIN_INTERVAL_MS + " milliseconds. "
										+ "It is likely that the pre-agg is in a good shape.");
							}
						} else {
							System.out.println(
								"[PreAggregateProcTimeListener] The parameter sent is not an integer: "
									+ message);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void cancel() {
		this.running = false;
	}

	private boolean isInteger(String s) {
		return isInteger(s, 10);
	}

	private boolean isInteger(String s, int radix) {
		if (s.isEmpty()) {
			return false;
		}
		for (int i = 0; i < s.length(); i++) {
			if (i == 0 && s.charAt(i) == '-') {
				if (s.length() == 1)
					return false;
				else
					continue;
			}
			if (Character.digit(s.charAt(i), radix) < 0)
				return false;
		}
		return true;
	}

	public long getIntervalMs() {
		return this.intervalMs;
	}

	public void setIntervalMs(long intervalMs) {
		this.intervalMs = intervalMs;
	}

	private void disclaimer() {
		System.out.println(
			"[PreAggregateProcTimeListener] started at [" + this.host + "] for subtask ["
				+ this.subtaskId + "]. It listens new frequency parameters from an MQTT broker.");
		System.out.println("[PreAggregateProcTimeListener] To publish on this broker use:");
		System.out.println(
			"[PreAggregateProcTimeListener] mosquitto_pub -h " + this.host + " -p " + this.port
				+ " -t " + this.topic + " -m \"intervalMs\"");
		System.out.println();
	}
}
