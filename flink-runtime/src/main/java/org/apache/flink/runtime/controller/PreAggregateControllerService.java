package org.apache.flink.runtime.controller;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The PreAggregate controller listens to all preAggregation operators metrics and publish a global pre-aggregate parameter
 * K on the preAggregation operators.
 */
public class PreAggregateControllerService extends Thread {

	protected static final int MIN_INTERVAL_MS = 50;
	private final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
	private final DecimalFormat df = new DecimalFormat("#.###");
	private final String TOPIC_PRE_AGG_PARAMETER = "topic-pre-aggregate-parameter";
	private final String TOPIC_PRE_AGG_STATE = "topic-pre-aggregate-state";
	private final int controllerFrequencySec;
	private final boolean running;
	private final PreAggregateSignalsListener preAggregateListener;
	private final String host;
	private final int port;
	// TODO: use Akka RPC instead of MQTT protocol
	private final Reference reference;
	/** MQTT broker is used to set the parameter K to all PreAgg operators */
	private MQTT mqtt;
	private FutureConnection connection;
	// global states
	private double numRecordsInPerSecondMax;
	private double numRecordsOutPerSecondMax;
	private int monitorCount;
	private boolean inputRecPerSecFlag;

	public PreAggregateControllerService() throws Exception {
		// Job manager and taskManager have to be deployed on the same machine, otherwise use the other constructor
		this("127.0.0.1");
	}

	public PreAggregateControllerService(String brokerServerHost) throws Exception {
		this.monitorCount = 0;
		this.inputRecPerSecFlag = false;
		this.numRecordsOutPerSecondMax = 0.0;
		this.controllerFrequencySec = 120; // 60 sec, 120 sec
		this.running = true;
		// 1 - define the reference for the output buffers: this.reference
		this.reference = new Reference(40, 65, 30, 85);

		if (Strings.isNullOrEmpty(brokerServerHost)
			|| brokerServerHost.equalsIgnoreCase("localhost")) {
			this.host = "127.0.0.1";
		} else if (brokerServerHost.contains("akka.tcp")) {
			this.host = extractIP(brokerServerHost);
		} else {
			this.host = "127.0.0.1";
		}
		this.port = 1883;

		this.preAggregateListener = new PreAggregateSignalsListener(
			this.host,
			this.port,
			TOPIC_PRE_AGG_STATE);
		this.preAggregateListener.start();
		this.disclaimer();
	}

	public static void main(String[] args) throws Exception {
		PreAggregateControllerService preAggregateControllerService = new PreAggregateControllerService(
			"akka.tcp://flink@127.0.0.1:6123/user/rpc/jobmanager_2");
		preAggregateControllerService.start();
	}

	private void disclaimer() {
		System.out.println(
			"[PreAggregateControllerService.controller] Controller started at [" + this.host +
				"] scheduled to every " + this.controllerFrequencySec + " seconds.");
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

	public void run() {
		try {
			if (mqtt == null) this.connect();
			while (running) {
				Thread.sleep(this.controllerFrequencySec * 1000);
				// Long newIntervalMs = computePreAggregateProcTimeIntervalMs();
				Long newIntervalMs = computeNextProcTimeIntervalMs();
				if (newIntervalMs != null && newIntervalMs >= MIN_INTERVAL_MS) {
					publish(newIntervalMs);
				} else {
					System.out.println(
						"[PreAggregateControllerService.controller] interval [" + newIntervalMs
							+ "] invalid. It is likely that the pre-agg is in a good shape.");
				}
			}
		} catch (Exception e) {
			System.out.println(
				"[PreAggregateControllerService.controller] FATAL ERROR: Controller is off!");
			e.printStackTrace();
		}
	}

	private Long computeNextProcTimeIntervalMs() {
		// @formatter:off
		System.out.println("[PreAggregateControllerService.controller] started at: " + sdf.format(new Date()));
		Long preAggregateIntervalMsNew = 0L;
		this.inputRecPerSecFlag = false;

		// 2 - collect the signals and compute the average
		PreAggregateGlobalState preAggregateGlobalState = computeAverageOfSignals();
		// 3 - check if at least one of the output buffers is 100%. This might be a skew workload.
		if (preAggregateGlobalState.isOverloaded()) {
			preAggregateGlobalState.incrementIntervalMsNew(200);
			preAggregateGlobalState.setValidate(true);
		}
		// 4 - check if the average output buffers are out of the reference. Then, compute the correction.
		else if (preAggregateGlobalState.getOutPoolUsageAvg() < reference.getMin() || preAggregateGlobalState.getOutPoolUsageAvg() > reference.getMax()) {
			// 4.1 - BACKPRESSURE: increment latency
			if (preAggregateGlobalState.getOutPoolUsageAvg() > reference.getMax()) {
				if (preAggregateGlobalState.getOutPoolUsageAvg() >= reference.getMaxHigh()) {
					preAggregateGlobalState.incrementIntervalMsNew(200);
				} else {
					preAggregateGlobalState.incrementIntervalMsNew(100);
				}
				preAggregateGlobalState.setValidate(true);
			}
			// 4.2 - TOO LOW PRESSURE: decrement latency
			else if (preAggregateGlobalState.getOutPoolUsageAvg() < reference.getMin()) {
				if (preAggregateGlobalState.getOutPoolUsageAvg() <= reference.getMinLow()) {
					preAggregateGlobalState.decrementIntervalMsNew(200);
				} else {
					preAggregateGlobalState.decrementIntervalMsNew(100);
				}
				preAggregateGlobalState.setValidate(true);
			}
			// 4.3 - should not fall here
			else { System.out.println("should not fall here"); }
		}
		// 5 - check if the average output buffers are within the reference. Then compute a small correction.
		else {
			System.out.println("[PreAggregateControllerService.controller] within the reference.");
		}
		// 6 - get the new intervalMs
		if (preAggregateGlobalState.isValidate()) {
			preAggregateIntervalMsNew = preAggregateGlobalState.getIntervalMsNew();
		}
		System.out.println("[PreAggregateControllerService.controller] Next global preAgg intervalMs: " + preAggregateIntervalMsNew);
		System.out.println("[PreAggregateControllerService.controller] done at: " + sdf.format(new Date()));
		return preAggregateIntervalMsNew;
		// @formatter:on
	}

	private PreAggregateGlobalState computeAverageOfSignals() {
		PreAggregateGlobalState preAggregateGlobalState = new PreAggregateGlobalState();
		int subtasksCount = 0;
		double outPoolUsageMeanTotal = 0;
		// double outPoolUsage75PerTotal = 0;
		for (Map.Entry<Integer, PreAggregateSignalsState> entry : this.preAggregateListener.preAggregateState
			.entrySet()) {
			// get the subtask ID
			Integer subtaskIndex = entry.getKey();
			PreAggregateSignalsState preAggregateState = entry.getValue();
			// get the current intervalMs and set on the global state
			preAggregateGlobalState.setIntervalMsCurrent(preAggregateState.getIntervalMs());
			// collect the output poll mean usage for each subtask
			double outPoolUsageMean = preAggregateState.getOutPoolUsageMean();
			outPoolUsageMeanTotal = outPoolUsageMeanTotal + outPoolUsageMean;
			double outPoolUsage75Perc = preAggregateState.getOutPoolUsage075();
			// check if this subtask is overloaded
			if (outPoolUsageMean >= 100.0 || outPoolUsage75Perc >= 100.0)
				preAggregateGlobalState.setOverloaded(true);
			// count the number of subtasks
			subtasksCount++;
			// update max throughput only if the pre-agg is in BACKPRESSURE
			if (outPoolUsageMean >= reference.getMax()) {
				updateGlobalCapacity(
					preAggregateState.getNumRecordsInPerSecond(),
					preAggregateState.getNumRecordsOutPerSecond());
			}
			// print the signals
			String msg = "[PreAggregateControllerService.controller] " + subtaskIndex +
				"|min:" + preAggregateState.getOutPoolUsageMin() +
				"|max:" + preAggregateState.getOutPoolUsageMax() +
				"|mean:" + preAggregateState.getOutPoolUsageMean() +
				"|50:" + preAggregateState.getOutPoolUsage05() +
				"|75:" + preAggregateState.getOutPoolUsage075() +
				"|95:" + preAggregateState.getOutPoolUsage095() +
				"|99:" + preAggregateState.getOutPoolUsage099() +
				"|stdD:" + df.format(preAggregateState.getOutPoolUsageStdDev()) +
				"|IN[" + df.format(preAggregateState.getNumRecordsInPerSecond()) +
				"|max:" + df.format(this.numRecordsInPerSecondMax) + "]" +
				"|OUT[" + df.format(preAggregateState.getNumRecordsOutPerSecond()) +
				"|max:" + df.format(this.numRecordsOutPerSecondMax) + "]|" +
				preAggregateState.getIntervalMs();
			System.out.println(msg);
		}
		// update the out poll usage average global (for all subtasks)
		preAggregateGlobalState.setOutPoolUsageAvg(outPoolUsageMeanTotal / subtasksCount);
		return preAggregateGlobalState;
	}

	/**
	 * @return
	 *
	 * @deprecated use computeNextProcTimeIntervalMs()
	 */
	private Long computePreAggregateProcTimeIntervalMs() {
		// @formatter:off
		System.out.println("[PreAggregateControllerService.controller] started at: " + sdf.format(new Date()));
		Long preAggregateIntervalMsNew = 0L;
		PreAggregateGlobalState preAggregateGlobalState = new PreAggregateGlobalState();
//		int preAggQtd = this.preAggregateListener.preAggregateState.size();
//		int preAggCount = 0;
		this.inputRecPerSecFlag = false;

		for (Map.Entry<Integer, PreAggregateSignalsState> entry : this.preAggregateListener.preAggregateState
			.entrySet()) {
//			preAggCount++;
			String label = "";
			Integer subtaskIndex = entry.getKey();
			PreAggregateSignalsState preAggregateState = entry.getValue();

			// get the current intervalMs and set on the global state
			preAggregateGlobalState.setIntervalMsCurrent(preAggregateState.getIntervalMs());

			// find the new global state to pre-aggregate
			if (preAggregateState.getOutPoolUsageMin() > 50.0 && preAggregateState.getOutPoolUsageMean() >= 60.0) {
				// BACKPRESSURE -> increase latency -> increase the pre-aggregation parameter
				if (preAggregateState.getOutPoolUsage05() == 100 && preAggregateState.getOutPoolUsageMax() == 100) {
					if (preAggregateState.getIntervalMs() <= 400) {
						preAggregateGlobalState.incrementIntervalMsNew(200);
						label = "+++";
					} else {
						// If it is the second time that we see a physical operator overloaded we increase the latency by 50%
						if (!preAggregateGlobalState.isOverloaded()) {
							preAggregateGlobalState.incrementIntervalMsNew(100);
						} else {
							preAggregateGlobalState.incrementIntervalMsNew(200);
						}
						label = "++";
					}
					preAggregateGlobalState.setOverloaded(true);
					// If half of the physical operator are overloaded (100%) we consider to increase latency anyway
					//if (preAggCount > (preAggQtd / 2)) {
					//	minCount.setOverloaded(true);
					//}
				} else {
					preAggregateGlobalState.incrementIntervalMsNew(100);
					label = "+";
					//if (this.numRecordsInPerSecondMax != 0 && preAggregateState.getNumRecordsInPerSecond() >= (this.numRecordsInPerSecondMax * 0.975)) {
					// If the input throughput is close to the max input throughput in 97,5% invalidate the increase latency action
					//	System.out.println("Controller: invalidating increasing latency (input)");
					//	minCount.setValidate(false);
					//}
				}
				if (this.numRecordsOutPerSecondMax != 0 && preAggregateState.getNumRecordsOutPerSecond() <= (
					this.numRecordsOutPerSecondMax * 0.90)) {
					// && (preAggregateState.getNumRecordsInPerSecond() >= (this.numRecordsInPerSecondMax * 0.975))
					// If the output throughput is lower than the 85% of the max input throughput invalidate the increase latency action
					System.out.println(
						"[PreAggregateControllerService.controller] invalidating increasing latency (output)");
					preAggregateGlobalState.setValidate(false);
				}
				this.updateGlobalCapacity(preAggregateState.getNumRecordsInPerSecond(), preAggregateState.getNumRecordsOutPerSecond());

			} else if (preAggregateState.getOutPoolUsageMin() <= 50.0 && preAggregateState.getOutPoolUsageMean() < 60.0) {
				// AVAILABLE RESOURCE -> minimize latency -> decrease the pre-aggregation parameter
				if (preAggregateState.getOutPoolUsageMin() <= 25 && preAggregateState.getOutPoolUsageMax() <= 25) {
					preAggregateGlobalState.decrementIntervalMsNew(100);
					label = "--";
				} else {
					// if the output throughput is greater than the capacity we don't decrease the parameter K
					if (this.numRecordsOutPerSecondMax == 0 || preAggregateState.getNumRecordsOutPerSecond() < this.numRecordsOutPerSecondMax) {
						preAggregateGlobalState.decrementIntervalMsNew(100);
						label = "-";
						if (preAggregateState.getNumRecordsOutPerSecond() >= (this.numRecordsOutPerSecondMax * 0.85)) {
							// If the output throughput is greater than the max output throughput in 85% invalidate the decrease latency action
							preAggregateGlobalState.setValidate(false);
						}
						if (preAggregateState.getNumRecordsInPerSecond() >= (this.numRecordsInPerSecondMax * 0.95)) {
							// If the input throughput is close to the max input throughput in 95% invalidate the decrease latency action
							preAggregateGlobalState.setValidate(false);
						}
					}
				}
			} else {
				if (preAggregateState.getNumRecordsInPerSecond() >= (this.numRecordsInPerSecondMax * 0.95)) {
					// this is the same lock of increasing and decreasing latency
					preAggregateGlobalState.setValidate(false);
				}
			}
			String msg = "[PreAggregateControllerService.controller] " + subtaskIndex +
				"|" + preAggregateState.getOutPoolUsageMin() + "|" + preAggregateState.getOutPoolUsageMax() + "|" + preAggregateState.getOutPoolUsageMean() + "|" + preAggregateState.getOutPoolUsage05() + "|" + preAggregateState.getOutPoolUsage075() + "|" + preAggregateState.getOutPoolUsage095() + "|" + preAggregateState.getOutPoolUsage099() + "|" + df.format(preAggregateState.getOutPoolUsageStdDev()) +
				"|IN[" + df.format(preAggregateState.getNumRecordsInPerSecond()) + "]max[" + df.format(this.numRecordsInPerSecondMax) +
				"]OUT[" + df.format(preAggregateState.getNumRecordsOutPerSecond()) + "]max[" + df.format(this.numRecordsOutPerSecondMax) + "]|" +
				preAggregateState.getIntervalMs() + "|" + label + "|" + preAggregateGlobalState.isValidate();
			System.out.println(msg);
		}
		if (preAggregateGlobalState.isOverloaded() || preAggregateGlobalState.isValidate()) {
			preAggregateIntervalMsNew = preAggregateGlobalState.getIntervalMsNew();
		}
		this.monitorCount++;
		System.out.println("[PreAggregateControllerService.controller] Next global preAgg intervalMs: " + preAggregateIntervalMsNew);
		System.out.println("[PreAggregateControllerService.controller] done at: " + sdf.format(new Date()));
		return preAggregateIntervalMsNew;
		// @formatter:on
	}

	private void updateGlobalCapacity(double numRecordsInPerSecond, double numRecordsOutPerSecond) {
		if (this.monitorCount >= 3) {
			// update Input throughput
			if (numRecordsInPerSecond > this.numRecordsInPerSecondMax) {
				this.numRecordsInPerSecondMax = numRecordsInPerSecond;
				this.inputRecPerSecFlag = true;
				this.monitorCount = 0;
			}
			// update Output throughput. Only update output if the input was not updated because it could be a spike or
			// a high data rate fluctuation on the channel
			if (!this.inputRecPerSecFlag
				&& numRecordsOutPerSecond > this.numRecordsOutPerSecondMax) {
				this.numRecordsOutPerSecondMax = numRecordsOutPerSecond;
			}
		}
	}

	private void publish(long newMaxCountPreAggregate) throws Exception {
		final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
		UTF8Buffer topic = new UTF8Buffer(TOPIC_PRE_AGG_PARAMETER);
		Buffer msg = new AsciiBuffer(Long.toString(newMaxCountPreAggregate));

		// Send the publish without waiting for it to complete. This allows us to send multiple message without blocking.
		queue.add(connection.publish(topic, msg, QoS.AT_LEAST_ONCE, false));

		while (!queue.isEmpty()) {
			queue.removeFirst().await();
		}
	}

	public String extractIP(String ipString) {
		String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
		Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
		Matcher matcher = pattern.matcher(ipString);
		if (matcher.find()) {
			return matcher.group();
		} else {
			return "127.0.0.1";
		}
	}

	private static class Reference {
		private final Integer min;
		private final Integer max;
		private final Integer minLow;
		private final Integer maxHigh;

		public Reference(Integer min, Integer max, Integer minLow, Integer maxHigh) {
			this.min = min;
			this.max = max;
			this.minLow = minLow;
			this.maxHigh = maxHigh;
		}

		public Integer getMin() {
			return min;
		}

		public Integer getMax() {
			return max;
		}

		public Integer getMinLow() {
			return minLow;
		}

		public Integer getMaxHigh() {
			return maxHigh;
		}
	}
}
