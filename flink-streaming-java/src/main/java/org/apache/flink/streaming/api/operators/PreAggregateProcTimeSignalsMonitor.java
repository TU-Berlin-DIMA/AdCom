package org.apache.flink.streaming.api.operators;

import org.apache.flink.metrics.Histogram;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import org.apache.flink.streaming.util.functions.PreAggIntervalMsGauge;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.LinkedList;

/**
 * This class listen to signals on the pre-agg operators and send them to the controller in the JobManager.
 */
public class PreAggregateProcTimeSignalsMonitor extends Thread implements Serializable {

	private final DecimalFormat df = new DecimalFormat("#.###");
	/** topi on the JobManager controller */
	private final String TOPIC_PRE_AGG_STATE = "topic-pre-aggregate-state";
	/** Histogram metrics to monitor network buffer */
	private final Histogram outPoolUsageHistogram;
	/** Gauge metrics to monitor latency parameter */
	private final PreAggIntervalMsGauge preAggIntervalMsGauge;
	private final long publishSignalsFrequencySec;
	private final int subtaskId;
	private final boolean enableController;
	private final String topic;
	private final String host;
	private final int port;
	// DANGER: this time has to be lower than the controller time on the runtime package [PreAggregateControllerService]
	private final long PUBLISH_SIGNALS_FREQ_SEC = 30 * 1000;
	// frequency to collect signals from the pre-agg operator
	private final long TIMEOUT_TO_COLLECT_SIGNALS = 30 * 1000;
	/** time elapse to the next monitoring collect of signals */
	private long timeStart;
	private long intervalMs;
	/** throughput of the operator */
	private double numRecordsOutPerSecond;
	// TODO: use Akka RPC instead of MQTT
	private double numRecordsInPerSecond;
	/** MQTT broker is used to send signals of each pre-agg operator to the JobManager controller */
	private MQTT mqtt;
	private FutureConnection connection;
	private boolean running = false;

	public PreAggregateProcTimeSignalsMonitor(
		long intervalMs,
		Histogram outPoolUsageHistogram,
		PreAggIntervalMsGauge preAggIntervalMsGauge,
		String jobManagerAddress,
		int subtaskId,
		boolean enableController) {

		this.intervalMs = intervalMs;
		this.outPoolUsageHistogram = outPoolUsageHistogram;
		this.preAggIntervalMsGauge = preAggIntervalMsGauge;
		this.running = true;
		this.publishSignalsFrequencySec = PUBLISH_SIGNALS_FREQ_SEC;
		this.subtaskId = subtaskId;
		this.enableController = enableController;

		this.topic = TOPIC_PRE_AGG_STATE;
		this.host = (Strings.isNullOrEmpty(jobManagerAddress) || jobManagerAddress.equalsIgnoreCase(
			"localhost")) ? "127.0.0.1" : jobManagerAddress;
		this.port = 1883;

		this.timeStart = System.currentTimeMillis();

		this.disclaimer();
	}

	private void disclaimer() {
		System.out.println("[PreAggregateProcTimeSignalsMonitor] started at [" + this.host
			+ "] for subtask [" + this.subtaskId
			+ "]. It collects and publishes signals to the pre-agg controller using an MQTT broker.");
		if (!this.enableController) {
			System.out.println(
				"[PreAggregateProcTimeSignalsMonitor] Controller is not enable then the monitor doesn't have to send signals.");
		}
		System.out.println();
	}

	private void connect() throws Exception {
		mqtt = new MQTT();
		mqtt.setHost(host, port);

		connection = mqtt.futureConnection();
		connection.connect().await();
	}

	private void disconnect() throws Exception {
		connection.disconnect().await();
	}

	@Override
	public void run() {
		try {
			if (mqtt == null) this.connect();
			while (running) {
				// Publish AdCom signals every #publishSignalsFrequencySec (30 seconds) to the Global PI Controller
				Thread.sleep(publishSignalsFrequencySec);
				String preAggSignals = this.updateSignals();
				this.publish(preAggSignals);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void publish(String message) throws Exception {
		final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
		UTF8Buffer topicUTF8 = new UTF8Buffer(topic);
		Buffer msg = new AsciiBuffer(message);

		// Send the publish without waiting for it to complete. This allows us to send multiple message without blocking.
		queue.add(connection.publish(topicUTF8, msg, QoS.AT_LEAST_ONCE, false));

		while (!queue.isEmpty()) {
			queue.removeFirst().await();
		}
	}

	private String updateSignals() {
		long outPoolUsageMin = this.outPoolUsageHistogram.getStatistics().getMin();
		long outPoolUsageMax = this.outPoolUsageHistogram.getStatistics().getMax();
		double outPoolUsageMean = this.outPoolUsageHistogram.getStatistics().getMean();
		double outPoolUsage05 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.5);
		double outPoolUsage075 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.75);
		double outPoolUsage095 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.95);
		double outPoolUsage099 = this.outPoolUsageHistogram.getStatistics().getQuantile(0.99);
		double outPoolUsageStdDev = this.outPoolUsageHistogram.getStatistics().getStdDev();

		String msg = subtaskId + "|" + outPoolUsageMin + "|" + outPoolUsageMax + "|" +
			outPoolUsageMean + "|" + outPoolUsage05 + "|" + outPoolUsage075 + "|" + outPoolUsage095
			+ "|" + outPoolUsage099 + "|" + outPoolUsageStdDev + "|" +
			df.format(this.numRecordsInPerSecond) + "|" +
			df.format(this.numRecordsOutPerSecond) + "|" +
			this.intervalMs;

		// Update parameters to Prometheus+Grafana
		this.preAggIntervalMsGauge.updateValue(this.intervalMs);

		return msg;
	}

	public void setIntervalMs(long intervalMs) {
		this.intervalMs = intervalMs;
	}

	public void cancel() {
		this.running = false;
	}

	public Histogram getOutPoolUsageHistogram() {
		return outPoolUsageHistogram;
	}

	public void setNumRecordsOutPerSecond(double numRecordsOutPerSecond) {
		this.numRecordsOutPerSecond = numRecordsOutPerSecond;
	}

	public void setNumRecordsInPerSecond(double numRecordsInPerSecond) {
		this.numRecordsInPerSecond = numRecordsInPerSecond;
	}

	public boolean collectNextSignals() {
		// Collect AdCom signals to the Reservoir Histogram every 30 seconds
		if (System.currentTimeMillis() >= this.timeStart + this.TIMEOUT_TO_COLLECT_SIGNALS) {
			this.timeStart = System.currentTimeMillis();
			return true;
		}
		return false;
	}
}
