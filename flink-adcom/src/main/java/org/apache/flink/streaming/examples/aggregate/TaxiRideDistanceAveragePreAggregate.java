package org.apache.flink.streaming.examples.aggregate;

public class TaxiRideDistanceAveragePreAggregate {
	public static void main(String[] args) throws Exception {
		// @formatter:off
		/*
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get(SOURCE, ExerciseBase.pathToRideData);
		boolean parallelSource = params.getBoolean(SOURCE_PARALLEL, false);
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		String output = params.get(SINK, "");
		int timeCharacteristic = params.getInt(TIME_CHARACTERISTIC, 0);
		long preAggregationProcessingTimer = params.getLong(PRE_AGGREGATE_WINDOW_TIMEOUT, -1);
		int slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);

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

		DataStream<TaxiRide> rides = null;
		if (parallelSource) {
			rides = env.addSource(new TaxiRideSourceParallel(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		} else {
			rides = env.addSource(new TaxiRideSource(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		}

		DataStream<Tuple2<Integer, Double>> tuples = rides.map(new TaxiRideDistanceTokenizerMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		PreAggregateFunction<Integer, Tuple2<Integer, Tuple2<Double, Long>>, Tuple2<Integer, Double>,
			Tuple2<Integer, Tuple2<Double, Long>>> taxiRidePreAggregateFunction = new TaxiRidePassengerSumPreAggregateFunction();
		DataStream<Tuple2<Integer, Tuple2<Double, Long>>> preAggregatedStream = null;
		if (!enableController && preAggregationProcessingTimer == -1) {
			// no combiner
			// preAggregatedStream = tuples;
		} else if (!enableController && preAggregationProcessingTimer > 0) {
			// static combiner based on timeout
			preAggregatedStream = tuples.combine(taxiRidePreAggregateFunction, preAggregationProcessingTimer).disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		} else if (enableController) {
			// dynamic combiner with PI controller
			preAggregatedStream = tuples.adCombine(taxiRidePreAggregateFunction, preAggregationProcessingTimer).disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		}

		KeyedStream<Tuple2<Integer, Tuple2<Double, Long>>, Integer> keyedByRandomDriver = preAggregatedStream.keyBy(new TaxiRideDriverKeySelector());

		DataStream<Tuple2<Integer, Tuple2<Double, Long>>> averagePassengers = keyedByRandomDriver
			.reduce(new TaxiRideAveragePassengersReducer()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			averagePassengers
				.map(new TaxiRideAverageOutputFlatMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else {
			averagePassengers
				.map(new TaxiRideAverageOutputFlatMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		}

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideDistanceAveragePreAggregate.class.getSimpleName());
		 */
		// @formatter:on
	}
}
