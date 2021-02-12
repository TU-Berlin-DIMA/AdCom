/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.aggregate;

/**
 * This is a dynamic pre-aggregator of items to be placed before the shuffle phase in a DAG. There are three types of use
 * case to test in this class.
 * First we test the DAG (word count example) without any pre-aggregator. Second, we test the pre-aggregator based only by
 * a time threshold. This is similar to a tumbling window. Finally, we test the pre-aggregator based on a time threshold
 * and on a max of items to aggregate. If the frequency of items is very high it is reasonable to shuffle data before the
 * time threshold to have timely results.
 *
 * <pre>
 * Changes the frequency that the pre-aggregate emits batches of data:
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "0"
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "10"
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "100"
 * mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-pre-aggregate -m "1000"
 *
 * usage: java TopNPreAggregate \
 *        -pre-aggregate-window [>0 items] \
 *        -strategy [GLOBAL, LOCAL, PER_KEY] \
 *        -input mqtt \
 *        -sourceHost [127.0.0.1] -sourcePort [1883] \
 *        -controller [false] \
 *        -topN [long] \
 *        -pooling 100 \ # pooling frequency from source if not using mqtt data source
 *        -output [mqtt|log|text] \
 *        -sinkHost [127.0.0.1] -sinkPort [1883] \
 *        -slotSplit [false] -disableOperatorChaining [false]
 *
 * Running on the IDE:
 * usage: java TopNPreAggregate -pre-aggregate-window 1 -strategy GLOBAL -input mqtt -output mqtt
 *
 * </pre>
 */
public class TopNPreAggregate {
	public static void main(String[] args) throws Exception {
		// Checking input parameters
		/*
		final ParameterTool params = ParameterTool.fromArgs(args);

		String input = params.get(SOURCE, "");
		String sourceHost = params.get(SOURCE_HOST, "127.0.0.1");
		int sourcePort = params.getInt(SOURCE_PORT, 1883);
		String output = params.get(SINK, "");
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		int poolingFrequency = params.getInt(POOLING_FREQUENCY, 0);
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 1);
		long preAggregationWindowTimer = params.getLong(PRE_AGGREGATE_WINDOW_TIMEOUT, -1);
		int topN = params.getInt(TOP_N, 10);
		int slotSplit = params.getInt(SLOT_GROUP_SPLIT, 0);
		int parallelisGroup02 = params.getInt(PARALLELISM_GROUP_02, ExecutionConfig.PARALLELISM_DEFAULT);
		long bufferTimeout = params.getLong(BUFFER_TIMEOUT, -999);
		boolean enableController = params.getBoolean(CONTROLLER, true);
		boolean disableOperatorChaining = params.getBoolean(DISABLE_OPERATOR_CHAINING, false);

		System.out.println("data source                                             : " + input);
		System.out.println("data source host:port                                   : " + sourceHost + ":" + sourcePort);
		System.out.println("data source topic                                       : " + TOPIC_DATA_SOURCE);
		System.out.println("data sink                                               : " + output);
		System.out.println("data sink host:port                                     : " + sinkHost + ":" + sinkPort);
		System.out.println("data sink topic                                         : " + TOPIC_DATA_SINK);
		System.out.println("Feedback loop Controller                                : " + enableController);
		System.out.println("Slot split 0-no split, 1-combiner, 2-combiner & reducer : " + slotSplit);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("pooling frequency [milliseconds]                        : " + poolingFrequency);
		System.out.println("pre-aggregate window [count]                            : " + preAggregationWindowCount);
		System.out.println("pre-aggregate window [seconds]                          : " + preAggregationWindowTimer);
		System.out.println("topN                                                    : " + topN);
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);
		System.out.println("BufferTimeout [milliseconds]                            : " + bufferTimeout);
		System.out.println("Changing pooling frequency of the data source:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-data-source -m \"100\"");
		System.out.println("Changing pre-aggregation frequency before shuffling:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-pre-aggregate-parameter -m \"100\"");

		/*
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		if (bufferTimeout != -999) {
			env.setBufferTimeout(bufferTimeout);
		}
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

		// get input data
		DataStream<String> rawSensorValues;
		if (Strings.isNullOrEmpty(input)) {
			rawSensorValues = env.addSource(new DataRateSource(new String[0], poolingFrequency)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		} else if (SOURCE_DATA_MQTT.equalsIgnoreCase(input)) {
			rawSensorValues = env.addSource(new MqttDataSource(TOPIC_DATA_SOURCE, sourceHost, sourcePort)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		} else {
			// read the text file from given input path
			rawSensorValues = env.readTextFile(params.get("input")).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		}

		// split up the lines in pairs (2-tuples) containing: (word,1)
		DataStream<Tuple2<Integer, Double>> sensorValues = rawSensorValues.flatMap(new SensorTokenizer()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		// Combine the stream
		DataStream<Tuple2<Integer, Double[]>> preAggregatedStream = null;
		if (preAggregationWindowCount == 0 && preAggregationWindowTimer == -1) {
			// no combiner
			// preAggregatedStream = sensorValues;
		} else if (enableController == false && preAggregationWindowTimer > 0) {
			// static combiner based on timeout
			PreAggregateConcurrentFunction<Integer, Double[], Tuple2<Integer, Double>, Tuple2<Integer, Double[]>> topNPreAggregateFunction = new TopNPreAggregateConcurrentFunction(topN);
			preAggregatedStream = sensorValues
				.combiner(topNPreAggregateFunction, preAggregationWindowTimer)
				.disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		} else {
			PreAggregateFunction<Integer, Double[], Tuple2<Integer, Double>, Tuple2<Integer, Double[]>> topNPreAggregateFunction = new TopNPreAggregateFunction(topN);
			preAggregatedStream = sensorValues
				.combiner(topNPreAggregateFunction, preAggregationWindowCount, enableController)
				.disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		}

		// group by the tuple field "0" and sum up tuple field "1"
		KeyedStream<Tuple2<Integer, Double[]>, Tuple> keyedStream = preAggregatedStream
			.keyBy(0);

		DataStream<Tuple2<Integer, Double[]>> resultStream = keyedStream
			.reduce(new TopNReduceFunction(topN)).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

		// emit result
		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			resultStream
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else if (output.equalsIgnoreCase(SINK_LOG)) {
			resultStream.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			resultStream
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.writeAsText(params.get("output")).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			resultStream
				.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);
		}

		System.out.println("Execution plan >>>");
		System.err.println(env.getExecutionPlan());
		// execute program
		env.execute(TopNPreAggregate.class.getSimpleName());

		 */
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************
	/*
	public static final class SensorTokenizer implements FlatMapFunction<String, Tuple2<Integer, Double>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<Integer, Double>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\|");
			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					String[] sensorIdAndValue = token.split(";");
					if (sensorIdAndValue.length == 2) {
						out.collect(new Tuple2<Integer, Double>(Integer.valueOf(sensorIdAndValue[0]), Double.valueOf(sensorIdAndValue[1])));
					} else {
						// System.out.println("WARNING: Sensor ID and VALUE do not match with pattern <Integer, Double>: " + sensorIdAndValue.toString());
					}
				}
			}
		}
	}

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************
	private static class TopNPreAggregateFunction
		extends PreAggregateFunction<Integer, Double[], Tuple2<Integer, Double>, Tuple2<Integer, Double[]>> {
		private final Double MIN_VALUE = -999999.9;
		private final int topN;

		public TopNPreAggregateFunction(int topN) {
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

	private static class TopNReduceFunction implements ReduceFunction<Tuple2<Integer, Double[]>> {
		private final int topN;

		public TopNReduceFunction(int topN) {
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

		public static void main(String[] main) {
			Double[] array01 = new Double[]{55.0, 44.0, 5.0, 15.0, 25.0};
			Double[] array02 = new Double[]{40.0, 50.0, 10.0, 20.0, 30.0};
			Double[] result = TopNReduceFunction.getTopNArray(array01, array02);
			System.out.println("Result: ");
			for (int i = 0; i < result.length; i++) {
				System.out.print(result[i] + " - ");
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
