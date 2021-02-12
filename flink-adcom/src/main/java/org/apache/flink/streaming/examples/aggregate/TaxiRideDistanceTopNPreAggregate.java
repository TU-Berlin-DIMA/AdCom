package org.apache.flink.streaming.examples.aggregate;

public class TaxiRideDistanceTopNPreAggregate {
	public static void main(String[] args) throws Exception {
		/*
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get(SOURCE, ExerciseBase.pathToRideData);
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		String output = params.get(SINK, "");
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 1);
		long preAggregationWindowTimer = params.getLong(PRE_AGGREGATE_WINDOW_TIMEOUT, -1);
		boolean enableCombiner = params.getBoolean(COMBINER, true);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		int topN = params.getInt(TOP_N, 10);
		int slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);

		System.out.println("Download data from:");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiRides.gz");
		System.out.println("wget http://training.ververica.com/trainingData/nycTaxiFares.gz");
		System.out.println("data source                                             : " + input);
		System.out.println("data sink                                               : " + output);
		System.out.println("data sink host:port                                     : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                                         : " + TOPIC_DATA_SINK);
		System.out.println("Feedback loop Controller                                : " + enableController);
		System.out.println("Slot split 0-no split, 1-combiner, 2-combiner & reducer : " + slotSplit);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("Enable combiner                                         : " + enableCombiner);
		System.out.println("pre-aggregate window [count]                            : " + preAggregationWindowCount);
		System.out.println("pre-aggregate window [seconds]                          : " + preAggregationWindowTimer);
		System.out.println("topN                                                    : " + topN);
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);
		System.out.println("Changing pre-aggregation frequency before shuffling:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-pre-aggregate-parameter -m \"100\"");
		System.out.println(DataRateListener.class.getSimpleName() + " class to read data rate from file [" + DataRateListener.DATA_RATE_FILE + "] in milliseconds.");
		System.out.println("This listener reads every 60 seconds only the first line from the data rate file.");
		System.out.println("Use the following command to change the nanoseconds data rate:");
		System.out.println("1000000 nanoseconds = 1 millisecond");
		System.out.println("1000000000 nanoseconds = 1000 milliseconds = 1 second");
		System.out.println("echo \"1000000000\" > " + DataRateListener.DATA_RATE_FILE);

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

		/*
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);

		DataStream<Tuple2<Integer, Double[]>> preAggregatedStream = null;
		if (!enableCombiner) {
			// no combiner
			DataStream<Tuple2<Integer, Double[]>> tuples = rides.map(new TokenizerNoCombinerMap(topN)).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);
			preAggregatedStream = tuples;
		} else {
			// enable combiner
			DataStream<Tuple2<Integer, Double>> tuples = rides.map(new TokenizerMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);
			if (enableController == false && preAggregationWindowTimer > 0) {
				// static combiner based on timeout
				PreAggregateConcurrentFunction<Integer, Double[], Tuple2<Integer, Double>,
					Tuple2<Integer, Double[]>> topNPreAggregateFunction = new TaxiRidePassengerTopNPreAggregateConcurrent(topN);
				preAggregatedStream = tuples
					.combiner(topNPreAggregateFunction, preAggregationWindowTimer)
					.disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
			} else {
				PreAggregateFunction<Integer, Double[], Tuple2<Integer, Double>,
					Tuple2<Integer, Double[]>> topNPreAggregateFunction = new TaxiRidePassengerTopNPreAggregate(topN);
				preAggregatedStream = tuples
					.combiner(topNPreAggregateFunction, preAggregationWindowCount, enableController)
					.disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
			}
		}
		KeyedStream<Tuple2<Integer, Double[]>, Integer> keyedByRandomDriver = preAggregatedStream.keyBy(new RandomDriverKeySelector());

		DataStream<Tuple2<Integer, Double[]>> taxiRideTopNDistances = keyedByRandomDriver
			.reduce(new TaxiRideDistanceTopNReduce(topN)).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			taxiRideTopNDistances
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else {
			taxiRideTopNDistances
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideDistanceTopNPreAggregate.class.getSimpleName());
		 */
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************
	/*
	private static class TokenizerMap implements MapFunction<TaxiRide, Tuple2<Integer, Double>> {
		private final Random random;

		public TokenizerMap() {
			random = new Random();
		}

		@Override
		public Tuple2<Integer, Double> map(TaxiRide ride) {
			// create random keys from 0 to 10 in order to average the passengers of all taxi drivers
			int low = 0;
			int high = 10;
			Integer result = random.nextInt(high - low) + low;
			Double distance = TaxiRideDistanceCalculator.distance(ride.startLat, ride.startLon, ride.endLat, ride.endLon, "K");
			return Tuple2.of(result, distance);
		}
	}

	private static class TokenizerNoCombinerMap implements MapFunction<TaxiRide, Tuple2<Integer, Double[]>> {
		private final Double MIN_VALUE = -1.0;
		private final Random random;
		private final int topN;

		public TokenizerNoCombinerMap(int topN) {
			this.random = new Random();
			this.topN = topN;
		}

		@Override
		public Tuple2<Integer, Double[]> map(TaxiRide ride) {
			// create random keys from 0 to 10 in order to average the passengers of all taxi drivers
			int low = 0;
			int high = 10;
			Integer result = random.nextInt(high - low) + low;
			Double distance = TaxiRideDistanceCalculator.distance(ride.startLat, ride.startLon, ride.endLat, ride.endLon, "K");
			Double[] value = new Double[this.topN];
			for (int i = 0; i < this.topN; i++) {
				if (i == this.topN - 1) {
					value[i] = distance;
				} else {
					value[i] = MIN_VALUE;
				}
			}
			return Tuple2.of(result, value);
		}
	}

	/**
	 * Count the number of values and sum them.
	 * Key (Integer): random-key
	 * Value (Integer, Double, Long): random-key, passengerCnt.sum, random-key.count
	 * Input (Integer, Double): random-key, passengerCnt
	 * Output (Integer, Double, Long): random-key, passengerCnt.sum, random-key.count
	 */
	/*
	private static class TaxiRidePassengerTopNPreAggregate extends PreAggregateFunction<Integer, Double[],
		Tuple2<Integer, Double>, Tuple2<Integer, Double[]>> {
		private final Double MIN_VALUE = -1.0;
		private final int topN;

		public TaxiRidePassengerTopNPreAggregate(int topN) {
			this.topN = topN;
		}

		@Override
		public Double[] addInput(@Nullable Double[] value, Tuple2<Integer, Double> input) throws Exception {
			if (value == null) {
				value = new Double[this.topN];
				for (int i = 0; i < this.topN; i++) {
					if (i == this.topN - 1) {
						value[i] = input.f1;
					} else {
						value[i] = MIN_VALUE;
					}
				}
			} else {
				Arrays.sort(value);
				for (int i = this.topN - 1; i >= 0; i--) {
					if (value[i] < input.f1) {
						value[i] = input.f1;
						break;
					}
				}
			}
			return value;
		}

		@Override
		public void collect(Map<Integer, Double[]> buffer, Collector<Tuple2<Integer, Double[]>> out) throws Exception {
			for (Map.Entry<Integer, Double[]> entry : buffer.entrySet()) {
				Double[] values = entry.getValue();
				out.collect(Tuple2.of(entry.getKey(), values));
			}
		}
	}

	private static class TaxiRidePassengerTopNPreAggregateConcurrent extends PreAggregateConcurrentFunction<Integer, Double[],
		Tuple2<Integer, Double>, Tuple2<Integer, Double[]>> {
		private final Double MIN_VALUE = -1.0;
		private final int topN;

		public TaxiRidePassengerTopNPreAggregateConcurrent(int topN) {
			this.topN = topN;
		}

		@Override
		public Double[] addInput(@Nullable Double[] value, Tuple2<Integer, Double> input) throws Exception {
			if (value == null) {
				value = new Double[this.topN];
				for (int i = 0; i < this.topN; i++) {
					if (i == this.topN - 1) {
						value[i] = input.f1;
					} else {
						value[i] = MIN_VALUE;
					}
				}
			} else {
				Arrays.sort(value);
				for (int i = this.topN - 1; i >= 0; i--) {
					if (value[i] < input.f1) {
						value[i] = input.f1;
						break;
					}
				}
			}
			return value;
		}

		@Override
		public void collect(ConcurrentMap<Integer, Double[]> buffer, Collector<Tuple2<Integer, Double[]>> out) throws Exception {
			for (Map.Entry<Integer, Double[]> entry : buffer.entrySet()) {
				Double[] values = entry.getValue();
				out.collect(Tuple2.of(entry.getKey(), values));
			}
		}
	}

	private static class RandomDriverKeySelector implements KeySelector<Tuple2<Integer, Double[]>, Integer> {
		@Override
		public Integer getKey(Tuple2<Integer, Double[]> value) throws Exception {
			return value.f0;
		}
	}

	private static class TaxiRideDistanceTopNReduce implements ReduceFunction<Tuple2<Integer, Double[]>> {

		private final int topN;

		public TaxiRideDistanceTopNReduce(int topN) {
			this.topN = topN;
		}

		private static Double[] getTopNArray(Double[] array01, Double[] array02) {
			if (array01.length != array02.length) {
				System.out.println("Arrays need to have the same length. array01[" + array01.length + "] array02[" + array02.length + "]");
			}
			Arrays.sort(array01);
			Arrays.sort(array02);
			int offset01 = array01.length - 1; // i is the offset from array01
			int offset02 = array02.length - 1; // j is the offset from array02
			// if the topN of array01 is greater than the topN of the array02 we weill use the array01 as the result
			if (array01[offset01] >= array02[offset02]) {
				while (offset01 >= 0) {
					while (offset02 >= 0 && offset01 >= 0) {
						if (array01[offset01] > array02[offset02]) {
							break;
						} else if (array02[offset02] > array01[offset01]) {
							Double swap = array01[offset01];
							array01[offset01] = array02[offset02];

							int i = offset01 - 1;
							while (i >= 0) {
								Double swapInner = array01[i];
								array01[i] = swap;
								swap = swapInner;
								i--;
							}
							offset01--;
						}
						offset02--;
					}
					offset01--;
				}
				return array01;
			} else {
				return getTopNArray(array02, array01);
			}
		}

		@Override
		public Tuple2<Integer, Double[]> reduce(Tuple2<Integer, Double[]> value1, Tuple2<Integer, Double[]> value2) throws Exception {
			return Tuple2.of(value1.f0, getTopNArray(value1.f1, value2.f1));
		}
	}

	private static class FlatOutputMap implements MapFunction<Tuple2<Integer, Double[]>, String> {
		@Override
		public String map(Tuple2<Integer, Double[]> value) throws Exception {
			String result = "";
			for (int i = 0; i < value.f1.length; i++) {
				result = value.f1[i] + ", " + result;
			}
			return value.f0 + " [" + result + "]";
		}
	}
	 */
}
