package org.apache.flink.streaming.examples.aggregate.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;

public class GenericParameters {

	private final String input;
	private final ParameterTool params;
	private final boolean parallelSource;
	private final String sinkHost;
	private final int sinkPort;
	private final String output;
	private final int timeCharacteristic;
	private final long preAggregationProcessingTimer;
	private final int slotSplit;
	private final int parallelisGroup02;
	private final boolean enableController;
	private final boolean disableOperatorChaining;
	private final boolean mini_batch_enabled;
	private final boolean distinct_agg_split;
	private final int parallelismTableApi;
	private final String mini_batch_allow_latency;
	private final int mini_batch_size;
	private final boolean twoPhaseAgg;

	public GenericParameters(String[] args) {
		// @formatter:off
		params = ParameterTool.fromArgs(args);
		input = params.get(SOURCE, ExerciseBase.pathToRideData);
		parallelSource = params.getBoolean(SOURCE_PARALLEL, false);
		sinkHost = params.get(SINK_HOST, "127.0.0.1");
		sinkPort = params.getInt(SINK_PORT, 1883);
		output = params.get(SINK, "");
		timeCharacteristic = params.getInt(TIME_CHARACTERISTIC, 0);
		preAggregationProcessingTimer = params.getLong(PRE_AGGREGATE_WINDOW_TIMEOUT, -1);
		slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		enableController = params.getBoolean(CONTROLLER, true);
		disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);
		mini_batch_enabled = params.getBoolean(TABLE_MINI_BATCH_ENABLE, false);
		distinct_agg_split = params.getBoolean(TABLE_DISTINCT_AGG_SPLIT_ENABLE, false);
		parallelismTableApi = params.getInt(TABLE_PARALLELISM, ExecutionConfig.PARALLELISM_DEFAULT);
		mini_batch_allow_latency = params.get(TABLE_MINI_BATCH_LATENCY, "").replace("_", " ");
		mini_batch_size = params.getInt(TABLE_MINI_BATCH_SIZE, 0);
		twoPhaseAgg = params.getBoolean(TABLE_MINI_BATCH_TWO_PHASE, false);
		// @formatter:on
	}

	public void printParameters() {
		// @formatter:off
		System.out.println("Download data from:");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiRides.gz");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiFares.gz");
		System.out.println("data source                                             : " + input);
		System.out.println("parallel source                                         : " + parallelSource);
		System.out.println("data sink                                               : " + output);
		System.out.println("data sink host:port                                     : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                                         : " + TOPIC_DATA_SINK);
		System.out.println("Slot split 0-no split, 1-combiner, 2-combiner & reducer : " + slotSplit);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("Feedback loop Controller                                : " + enableController);
		System.out.println("time characteristic 1-Processing 2-Event 3-Ingestion    : " + timeCharacteristic);
		System.out.println("pre-aggregate window [milliseconds]                     : " + preAggregationProcessingTimer);
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);
		System.out.println("Table API: mini-batch.enable                            : " + mini_batch_enabled);
		System.out.println("Table API: distinct-agg.split.enabled                   : " + distinct_agg_split);
		System.out.println("Table API: parallelism                                  : " + parallelismTableApi);
		System.out.println("Table API: mini-batch.latency                           : " + mini_batch_allow_latency);
		System.out.println("Table API: mini_batch.size                              : " + mini_batch_size);
		System.out.println("Table API: mini_batch.two_phase                         : " + twoPhaseAgg);
		// @formatter:on
	}

	public String getInput() {
		return input;
	}

	public ParameterTool getParams() {
		return params;
	}

	public boolean isParallelSource() {
		return parallelSource;
	}

	public String getSinkHost() {
		return sinkHost;
	}

	public int getSinkPort() {
		return sinkPort;
	}

	public String getOutput() {
		return output;
	}

	public int getTimeCharacteristic() {
		return timeCharacteristic;
	}

	public long getPreAggregationProcessingTimer() {
		return preAggregationProcessingTimer;
	}

	public int getSlotSplit() {
		return slotSplit;
	}

	public int getParallelisGroup02() {
		return parallelisGroup02;
	}

	public boolean isEnableController() {
		return enableController;
	}

	public boolean isDisableOperatorChaining() {
		return disableOperatorChaining;
	}

	public boolean isMini_batch_enabled() {
		return mini_batch_enabled;
	}

	public boolean isDistinct_agg_split_enabled() {
		return distinct_agg_split;
	}

	public int getParallelismTableApi() {
		return parallelismTableApi;
	}

	public String getMini_batch_allow_latency() {
		return mini_batch_allow_latency;
	}

	public int getMini_batch_size() {
		return mini_batch_size;
	}

	public boolean isTwoPhaseAgg() {
		return twoPhaseAgg;
	}
}
