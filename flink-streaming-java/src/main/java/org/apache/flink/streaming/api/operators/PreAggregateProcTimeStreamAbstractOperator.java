package org.apache.flink.streaming.api.operators;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.util.functions.PreAggIntervalMsGauge;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class PreAggregateProcTimeStreamAbstractOperator<K, V, IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT>, ProcessingTimeCallback {

	// @formatter:off
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
	private static final long serialVersionUID = 1L;
	/** metrics to monitor the PreAggregate operator */
	private final String PRE_AGGREGATE_OUT_POOL_USAGE_HISTOGRAM = "pre-aggregate-outPoolUsage-histogram";
	private final String PRE_AGGREGATE_PARAMETER = "pre-aggregate-parameter";
	/** The function used to process when receiving element. */
	private final PreAggregateFunction<K, V, IN, OUT> function;
	/** controller properties, processing time to trigger the preAggregate function*/
	private final long initialIntervalMs;
	private final boolean enableController;
	private PreAggregateProcTimeListener preAggregateProcTimeListener;
	private transient long currentWatermark;
	/** The map in heap to store elements. */
	private transient Map<K, V> bundle;
	/** Output for stream records. */
	private transient Collector<OUT> collector;
	/** The PreAggregate monitor to send signals to the PI controller on the JobManager */
	private PreAggregateProcTimeSignalsMonitor preAggregateMonitor;
	// @formatter:on

	public PreAggregateProcTimeStreamAbstractOperator(
		PreAggregateFunction<K, V, IN, OUT> function,
		long intervalMs,
		boolean enableController) {
		this.function = checkNotNull(function, "function is null");
		this.initialIntervalMs = intervalMs;
		this.enableController = enableController;
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.collector = new TimestampedCollector<>(output);
		this.bundle = new HashMap<>();

		currentWatermark = 0;

		// Find the JobManager address
		String jobManagerAddress = getRuntimeContext()
			.getTaskManagerRuntimeInfo()
			.getConfiguration()
			.getValue(JobManagerOptions.ADDRESS);

		this.preAggregateProcTimeListener = new PreAggregateProcTimeListener(
			jobManagerAddress,
			initialIntervalMs,
			getRuntimeContext().getIndexOfThisSubtask(),
			this.enableController);

		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(
			now + preAggregateProcTimeListener.getIntervalMs(), this);

		// Metrics to send to the controller
		// DANGER: the reservoirWindow time (seconds) has to be at least 2 times greater than
		// the PreAggregateProcTimeSignalsMonitor frequency to read signals,
		// otherwise the histogram gets empty.
		int reservoirWindow = 120;
		com.codahale.metrics.Histogram dropwizardOutPoolBufferHistogram = new com.codahale.metrics.Histogram(
			new SlidingTimeWindowArrayReservoir(reservoirWindow, TimeUnit.SECONDS));
		Histogram outPoolUsageHistogram = getRuntimeContext().getMetricGroup().histogram(
			PRE_AGGREGATE_OUT_POOL_USAGE_HISTOGRAM,
			new DropwizardHistogramWrapper(dropwizardOutPoolBufferHistogram));
		PreAggIntervalMsGauge preAggIntervalMsGauge = getRuntimeContext()
			.getMetricGroup()
			.gauge(PRE_AGGREGATE_PARAMETER, new PreAggIntervalMsGauge());

		// initiate the Controller-monitor with the histogram metrics for each pre-aggregate operator instance
		this.preAggregateMonitor = new PreAggregateProcTimeSignalsMonitor(
			initialIntervalMs,
			outPoolUsageHistogram,
			preAggIntervalMsGauge,
			jobManagerAddress,
			getRuntimeContext().getIndexOfThisSubtask(),
			this.enableController);

		// start the monitor of pre-agg signals
		this.preAggregateMonitor.start();
		// start the preAgg listener to receive new time processing parameter to the pre-agg
		this.preAggregateProcTimeListener.start();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// get the key and value for the map bundle
		final IN input = element.getValue();
		final K bundleKey = getKey(input);
		final V bundleValue = this.bundle.get(bundleKey);

		// get a new value after adding this element to bundle
		final V newBundleValue = this.function.addInput(bundleValue, input);

		// update to map bundle
		this.bundle.put(bundleKey, newBundleValue);
	}

	/**
	 * Get the key for current processing element, which will be used as the map bundle's key.
	 */
	protected abstract K getKey(final IN input) throws Exception;

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		long currentProcessingTime = getProcessingTimeService().getCurrentProcessingTime();
		this.collect();
		// getProcessingTimeService().registerTimer(currentProcessingTime + intervalMs, this);
		// System.out.println(PreAggregateProcTimeStreamAbstractOperator.class.getSimpleName() + ".onProcessingTime: " + sdf.format(new Timestamp(System.currentTimeMillis())));
		getProcessingTimeService().registerTimer(
			currentProcessingTime + preAggregateProcTimeListener.getIntervalMs(), this);
//		System.out.println("[PreAggregateProcTimeStreamAbstractOperator] intervalMs: " + preAggregateProcTimeListener.getIntervalMs() + " - " +
//			PreAggregateProcTimeStreamAbstractOperator.class.getSimpleName() + ".onProcessingTime: "
//			+ sdf.format(new Timestamp(System.currentTimeMillis())));

		CompletableFuture.runAsync(() -> {
			// Collect AdCom signals to the Reservoir Histogram every 30 seconds
			if (this.preAggregateMonitor.collectNextSignals()) {
				// update IntervalMs to Prometheus+Grafana
				this.preAggregateMonitor.setIntervalMs(preAggregateProcTimeListener.getIntervalMs());
				// update outPoolUsage metrics to Prometheus+Grafana
				float outPoolUsage = 0.0f;
				OperatorMetricGroup operatorMetricGroup = (OperatorMetricGroup) this.getMetricGroup();
				TaskMetricGroup taskMetricGroup = operatorMetricGroup.parent();
				MetricGroup metricGroup = taskMetricGroup.getGroup("buffers");
				Gauge<Float> gaugeOutPoolUsage = (Gauge<Float>) metricGroup.getMetric("outPoolUsage");
				if (gaugeOutPoolUsage != null && gaugeOutPoolUsage.getValue() != null) {
					outPoolUsage = gaugeOutPoolUsage.getValue().floatValue();
					this.preAggregateMonitor
						.getOutPoolUsageHistogram()
						.update((long) (outPoolUsage * 100));
				}
				// update records_per_second metrics to Prometheus+Grafana
				MeterView meterNumRecordsOutPerSecond = (MeterView) taskMetricGroup.getMetric(
					"numRecordsOutPerSecond");
				MeterView meterNumRecordsInPerSecond = (MeterView) taskMetricGroup.getMetric(
					"numRecordsInPerSecond");
				if (meterNumRecordsOutPerSecond != null) {
					this.preAggregateMonitor.setNumRecordsOutPerSecond(meterNumRecordsOutPerSecond.getRate());
				}
				if (meterNumRecordsInPerSecond != null) {
					this.preAggregateMonitor.setNumRecordsInPerSecond(meterNumRecordsInPerSecond.getRate());
				}
			}
		});

	}

	private void collect() throws Exception {
		if (!this.bundle.isEmpty()) {
			this.function.collect(bundle, collector);
			this.bundle.clear();
		}
	}

	@Override
	public void close() throws Exception {
		try {
			this.collect();
		} finally {
			Exception exception = null;

			try {
				super.close();
				if (function != null) {
					FunctionUtils.closeFunction(function);
				}
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the BundleOperator.", exception);
			}
		}
	}
}
