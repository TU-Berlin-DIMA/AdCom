package org.apache.flink.runtime.controller;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PreAggregateSignalsListener extends Thread {

	// This is a Map to store state of each pre-agg physical operator using the subtaskIndex as the key
	public final Map<Integer, PreAggregateSignalsState> preAggregateState;

	// Properties for the MQTT listen channel
	private final String topic;
	private final String host;
	private final int port;
	private BlockingConnection subscriber;
	private MQTT mqtt;
	private boolean running = false;

	public PreAggregateSignalsListener(String host, int port, String topic) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.running = true;
		this.preAggregateState = new HashMap<Integer, PreAggregateSignalsState>();
	}

	private void connect() throws Exception {
		this.mqtt = new MQTT();
		this.mqtt.setHost(host, port);
		this.subscriber = mqtt.blockingConnection();
		this.subscriber.connect();
		Topic[] topics = new Topic[]{new Topic(this.topic, QoS.AT_LEAST_ONCE)};
		this.subscriber.subscribe(topics);
	}

	public void cancel() {
		this.running = false;
	}

	public void run() {
		try {
			if (this.mqtt == null) this.connect();
			while (running) {
				// System.out.println("waiting for messages...");
				Message msg = subscriber.receive(10, TimeUnit.SECONDS);
				if (msg != null) {
					msg.ack();
					String message = new String(msg.getPayload(), UTF_8);
					if (message != null) {
						// Look at PreAggregateControllerService.computeAverageOfSignals() that receives the same message
						// System.out.println("[PreAggregateSignalsListener.controller] received msg: " + message);
						this.addState(message);
					} else {
						System.out.println(
							"[PreAggregateSignalsListener.controller] The parameter sent is null.");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void addState(String msg) {
		String[] states = msg.split("\\|");
		if (states != null && states.length == 12) {
			String subtaskIndex = states[0];
			String outPoolUsageMin = states[1];
			String outPoolUsageMax = states[2];
			String outPoolUsageMean = states[3];
			String outPoolUsage05 = states[4];
			String outPoolUsage075 = states[5];
			String outPoolUsage095 = states[6];
			String outPoolUsage099 = states[7];
			String outPoolUsageStdDev = states[8];
			String numRecordsInPerSecond = states[9];
			String numRecordsOutPerSecond = states[10];
			String intervalMs = states[11];

			PreAggregateSignalsState state = this.preAggregateState.get(Integer.parseInt(
				subtaskIndex));
			if (state == null) {
				state = new PreAggregateSignalsState(
					subtaskIndex,
					outPoolUsageMin,
					outPoolUsageMax,
					outPoolUsageMean,
					outPoolUsage05,
					outPoolUsage075,
					outPoolUsage095,
					outPoolUsage099,
					outPoolUsageStdDev,
					numRecordsInPerSecond,
					numRecordsOutPerSecond,
					intervalMs);
			} else {
				state.update(
					subtaskIndex,
					outPoolUsageMin,
					outPoolUsageMax,
					outPoolUsageMean,
					outPoolUsage05,
					outPoolUsage075,
					outPoolUsage095,
					outPoolUsage099,
					outPoolUsageStdDev,
					numRecordsInPerSecond,
					numRecordsOutPerSecond,
					intervalMs);
			}
			this.preAggregateState.put(Integer.parseInt(subtaskIndex), state);
		} else {
			System.out.println(
				"[PreAggregateSignalsListener.controller] ERROR: wrong number of parameter to update pre-aggregate state.");
		}
	}
}
