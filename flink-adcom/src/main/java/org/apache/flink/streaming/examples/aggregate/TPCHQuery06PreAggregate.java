package org.apache.flink.streaming.examples.aggregate;

/**
 * 6 Q6 - Forecasting Revenue Change Query
 * https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Sample%20querys.20.xml
 */
public class TPCHQuery06PreAggregate {
	public static void main(String[] args) throws Exception {
		// @formatter:off
		/*
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get(SOURCE, TPCH_DATA_LINE_ITEM);
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		String output = params.get(SINK, "");
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 0);
		int slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);

		System.out.println("Download data from:");
		System.out.println("https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Data%20generation%20tool.30.xml");
		System.out.println("data source                                             : " + input);
		System.out.println("data sink                                               : " + output);
		System.out.println("data sink host:port                                     : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                                         : " + TOPIC_DATA_SINK);
		System.out.println("Feedback loop Controller                                : " + enableController);
		System.out.println("Slot split 0-no split, 1-combiner, 2-combiner & reducer : " + slotSplit);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("pre-aggregate window [count]                            : " + preAggregationWindowCount);
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);
		System.out.println("Changing pre-aggregation frequency before shuffling:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-pre-aggregate-parameter -m \"100\"");
		System.out.println(DataRateListener.class.getSimpleName() + " class to read data rate from file [" + DataRateListener.DATA_RATE_FILE + "] in milliseconds.");
		System.out.println("This listener reads every 60 seconds only the first line from the data rate file.");
		System.out.println("Use the following command to change the nanoseconds data rate:");
		System.out.println("1000000 nanoseconds = 1 millisecond and 1000000000 nanoseconds = 1000 milliseconds = 1 second");
		System.out.println("500 nanoseconds   = 2M rec/sec");
		System.out.println("1000 nanoseconds  = 1M rec/sec");
		System.out.println("2000 nanoseconds  = 500K rec/sec");
		System.out.println("5000 nanoseconds  = 200K rec/sec");
		System.out.println("10000 nanoseconds = 100K rec/sec");
		System.out.println("20000 nanoseconds = 50K rec/sec");
		System.out.println("echo \"1000\" > " + DataRateListener.DATA_RATE_FILE);
		// @formatter:on

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}
		String slotGroup01 = SLOT_GROUP_DEFAULT;
		String slotGroup02 = SLOT_GROUP_DEFAULT;
		if (slotSplit == 0) {
			slotGroup01 = SLOT_GROUP_DEFAULT;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (slotSplit == 1) {
			slotGroup01 = SLOT_GROUP_01_01;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (slotSplit == 2) {
			slotGroup01 = SLOT_GROUP_01_01;
			slotGroup02 = SLOT_GROUP_01_02;
		}

		DataStream<LineItem> lineItems = env
			.addSource(new LineItemSource())
			.name(OPERATOR_SOURCE)
			.uid(OPERATOR_SOURCE)
			.slotSharingGroup(slotGroup01);

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			lineItems
				.map(new LineItemsOutMap())
				.name(OPERATOR_FLAT_OUTPUT)
				.uid(OPERATOR_FLAT_OUTPUT)
				.slotSharingGroup(slotGroup02)
				.setParallelism(parallelisGroup02)
				.print()
				.name(OPERATOR_SINK)
				.uid(OPERATOR_SINK)
				.slotSharingGroup(slotGroup02)
				.setParallelism(parallelisGroup02);
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			lineItems
				.map(new LineItemsOutMap())
				.name(OPERATOR_FLAT_OUTPUT)
				.uid(OPERATOR_FLAT_OUTPUT)
				.slotSharingGroup(slotGroup02)
				.setParallelism(parallelisGroup02)
				.print()
				.name(OPERATOR_SINK)
				.uid(OPERATOR_SINK)
				.slotSharingGroup(slotGroup02)
				.setParallelism(parallelisGroup02);
		} else {
			System.out.println("discarding output");
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TPCHQuery06PreAggregate.class.getSimpleName());
		 */
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************
	/*
	private static class TokenizerMap implements MapFunction<TaxiRide, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(TaxiRide ride) {
			return new Tuple2<Long, Long>(ride.driverId, 1L);
		}
	}

	private static class TaxiRideCountPreAggregateFunction
		extends PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public Long addInput(@Nullable Long value, Tuple2<Long, Long> input) throws Exception {
			if (value == null) {
				return input.f1;
			} else {
				return value + input.f1;
			}
		}

		@Override
		public void collect(
			Map<Long, Long> buffer,
			Collector<Tuple2<Long, Long>> out) throws Exception {
			for (Map.Entry<Long, Long> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	private static class SumReduceFunction implements ReduceFunction<Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
			return Tuple2.of(value1.f0, value1.f1 + value2.f1);
		}
	}

	private static class LineItemsOutMap implements MapFunction<LineItem, String> {
		@Override
		public String map(LineItem lineItem) {
			return lineItem.getOrderKey() + "|" +
				lineItem.getPartKey() + "|" + lineItem.getSupplierKey() + "|"
				+ lineItem.getLineNumber() + "|" +
				lineItem.getQuantity() + "|" + lineItem.getExtendedPrice() + "|"
				+ lineItem.getDiscount() + "|" +
				lineItem.getTax() + "|" + lineItem.getReturnFlag() + "|" + lineItem.getStatus()
				+ "|" +
				lineItem.getShipDate() + "|" + lineItem.getCommitDate() + "|"
				+ lineItem.getReceiptDate() + "|" +
				lineItem.getShipInstructions() + "|" + lineItem.getShipMode() + "|"
				+ lineItem.getComment();
		}
	}
	 */
}
