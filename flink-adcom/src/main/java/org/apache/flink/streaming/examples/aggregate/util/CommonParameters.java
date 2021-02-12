package org.apache.flink.streaming.examples.aggregate.util;

public class CommonParameters {
	// operator names
	public static final String OPERATOR_SOURCE = "source";
	public static final String OPERATOR_TOKENIZER = "tokenizer";
	public static final String OPERATOR_REDUCER = "reducer";
	public static final String OPERATOR_PRE_AGGREGATE = "pre-aggregate";
	public static final String OPERATOR_FLAT_OUTPUT = "flat-output";
	public static final String OPERATOR_SINK = "sink";

	// source parameters
	public static final String TPCH_DATA_LINE_ITEM = "/home/flink/tpch-dbgen/data/lineitem.tbl";
	public static final String TPCH_DATA_ORDER = "/home/flink/tpch-dbgen/data/orders.tbl";
	public static final String TPCH_DATA_COSTUMER = "/home/flink/tpch-dbgen/data/customer.tbl";
	public static final String TPCH_DATA_NATION = "/home/flink/tpch-dbgen/data/nation.tbl";
	public static final String SOURCE = "input";
	public static final String SOURCE_PARALLEL = "input-par";
	public static final String TOPIC_DATA_SOURCE = "topic-data-source";
	public static final String SOURCE_DATA_MQTT = "mqtt";
	public static final String SOURCE_HOST = "sourceHost";
	public static final String SOURCE_PORT = "sourcePort";
	public static final String SOURCE_WORDS = "words";
	public static final String SOURCE_SKEW_WORDS = "skew";
	public static final String SOURCE_FEW_WORDS = "few";
	public static final String SOURCE_DATA_RATE_VARIATION_WORDS = "variation";
	public static final String SOURCE_DATA_HAMLET = "hamlet";
	public static final String SOURCE_DATA_MOBY_DICK = "mobydick";
	public static final String SOURCE_DATA_DICTIONARY = "dictionary";
	// sink parameters
	public static final String SINK_HOST = "sinkHost";
	public static final String SINK_PORT = "sinkPort";
	public static final String SINK = "output";
	public static final String SINK_DATA_MQTT = "mqtt";
	public static final String SINK_LOG = "log";
	public static final String SINK_TEXT = "text";
	public static final String TOPIC_DATA_SINK = "topic-data-sink";
	// other parameters
	public static final String PARALLELISM_GROUP_01 = "parallelism-group-01";
	public static final String PARALLELISM_GROUP_02 = "parallelism-group-02";
	public static final String TABLE_PARALLELISM = "parallelism-table";
	public static final String SLOT_GROUP_DEFAULT = "default";
	public static final String SLOT_GROUP_01 = "group-01";
	public static final String SLOT_GROUP_02 = "group-02";
	public static final String MAX_COUNT_SOURCE = "maxCount";
	public static final String PRE_AGGREGATE_WINDOW = "pre-aggregate-window";
	public static final String PRE_AGGREGATE_WINDOW_TIMEOUT = "pre-aggregate-window-timeout";
	public static final String PRE_AGGREGATE_STRATEGY = "strategy";
	public static final String SLOT_GROUP_SPLIT = "slotSplit";
	public static final String DISABLE_OPERATOR_CHAINING = "disableOperatorChaining";
	public static final String TABLE_MINI_BATCH_ENABLE = "mini_batch_enabled";
	public static final String TABLE_DISTINCT_AGG_SPLIT_ENABLE = "distinct_agg_split_enabled";
	public static final String TABLE_MINI_BATCH_LATENCY = "mini_batch_latency";
	public static final String TABLE_MINI_BATCH_SIZE = "mini_batch_size";
	public static final String TABLE_MINI_BATCH_TWO_PHASE = "mini_batch_two_phase";
	public static final String ENABLE_END_TO_END_LATENCY_MONITOR = "enableEndToEndLatency";
	public static final String CONTROLLER = "controller";
	public static final String COMBINER = "combiner";
	public static final String LATENCY_TRACKING_INTERVAL = "latencyTrackingInterval";
	public static final String SIMULATE_SKEW = "simulateSkew";
	public static final String WINDOW = "window";
	public static final String SYNTHETIC_DELAY = "delay";
	public static final String BUFFER_TIMEOUT = "bufferTimeout";
	public static final String POOLING_FREQUENCY = "pooling";
	public static final String TOP_N = "topN";
	public static final String TIME_CHARACTERISTIC = "timeCharacteristic";
}
