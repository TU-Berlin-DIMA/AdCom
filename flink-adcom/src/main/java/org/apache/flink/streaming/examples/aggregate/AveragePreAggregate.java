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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;

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
 * usage: java AveragePreAggregate \
 *        -pre-aggregate-window [>0 items] \
 *        -strategy [GLOBAL, LOCAL, PER_KEY] \
 *        -input mqtt \
 *        -sourceHost [127.0.0.1] -sourcePort [1883] \
 *        -controller [true] \
 *        -pooling 100 \ # pooling frequency from source if not using mqtt data source
 *        -output [mqtt|log|text] \
 *        -sinkHost [127.0.0.1] -sinkPort [1883] \
 *        -slotSplit [false] -disableOperatorChaining [false]
 *
 * Running on the IDE:
 * usage: java AveragePreAggregate -pre-aggregate-window 1 -strategy GLOBAL -input mqtt -output mqtt
 *
 * </pre>
 */
public class AveragePreAggregate {
	public static void main(String[] args) throws Exception {
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		String input = params.get(SOURCE, "");
		String sourceHost = params.get(SOURCE_HOST, "127.0.0.1");
		int sourcePort = params.getInt(SOURCE_PORT, 1883);
		String output = params.get(SINK, "");
		String sinkHost = params.get(SINK_HOST, "127.0.0.1");
		int sinkPort = params.getInt(SINK_PORT, 1883);
		int poolingFrequency = params.getInt(POOLING_FREQUENCY, 0);
		int preAggregationWindowCount = params.getInt(PRE_AGGREGATE_WINDOW, 1);
		long preAggregationWindowTimer = params.getLong(PRE_AGGREGATE_WINDOW_TIMEOUT, -1);
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
		System.out.println("Parallelism group 02                                    : " + parallelisGroup02);
		System.out.println("Disable operator chaining                               : " + disableOperatorChaining);
		System.out.println("pooling frequency [milliseconds]                        : " + poolingFrequency);
		System.out.println("pre-aggregate window [count]                            : " + preAggregationWindowCount);
		System.out.println("pre-aggregate window [seconds]                          : " + preAggregationWindowTimer);
		System.out.println("BufferTimeout [milliseconds]                            : " + bufferTimeout);
		System.out.println("Changing pooling frequency of the data source:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-data-source -m \"100\"");
		System.out.println("Changing pre-aggregation frequency before shuffling:");
		System.out.println("mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-pre-aggregate-parameter -m \"100\"");

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
			slotGroup01 = SLOT_GROUP_01;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (slotSplit == 2) {
			slotGroup01 = SLOT_GROUP_01;
			slotGroup02 = SLOT_GROUP_02;
		}

		/*
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
		DataStream<Tuple2<Integer, Tuple2<Double, Integer>>> preAggregatedStream = null;
		if (preAggregationWindowCount == 0 && preAggregationWindowTimer == -1) {
			// no combiner
			preAggregatedStream = sensorValues.map(new SensorMapFunction());
		} else if (enableController == false && preAggregationWindowTimer > 0) {
			// static combiner based on timeout
			PreAggregateConcurrentFunction<Integer, Tuple2<Integer, Tuple2<Double, Integer>>, Tuple2<Integer, Double>,
				Tuple2<Integer, Tuple2<Double, Integer>>> sumPreAggregateFunction = new SensorValuesSumPreAggregateConcurrentFunction();
			preAggregatedStream = sensorValues
				.combiner(sumPreAggregateFunction, preAggregationWindowTimer)
				.disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		} else {
			// dynamic combiner with PI controller
			PreAggregateFunction<Integer, Tuple2<Integer, Tuple2<Double, Integer>>, Tuple2<Integer, Double>,
				Tuple2<Integer, Tuple2<Double, Integer>>> sumPreAggregateFunction = new SensorValuesSumPreAggregateFunction();
			preAggregatedStream = sensorValues
				.combiner(sumPreAggregateFunction, preAggregationWindowCount, enableController)
				.disableChaining().name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		}

		// group by the tuple field "0" and sum up tuple field "1"
		KeyedStream<Tuple2<Integer, Tuple2<Double, Integer>>, Tuple> keyedStream = preAggregatedStream.keyBy(0);

		DataStream<Tuple2<Integer, Tuple2<Double, Integer>>> resultStream = keyedStream.reduce(new AverageReduceFunction())
			.name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

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
		}
		resultStream
			.map(new FlatOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02)
			.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(parallelisGroup02);

		System.out.println("Execution plan >>>");
		System.err.println(env.getExecutionPlan());
		// execute program
		env.execute(AveragePreAggregate.class.getSimpleName());

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

	private static class SensorMapFunction implements MapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Tuple2<Double, Integer>>> {
		@Override
		public Tuple2<Integer, Tuple2<Double, Integer>> map(Tuple2<Integer, Double> value) throws Exception {
			return Tuple2.of(value.f0, Tuple2.of(value.f1, 1));
		}
	}
	 */

	// *************************************************************************
	// GENERIC merge function
	// *************************************************************************

	/**
	 * Count the number of values and sum them.
	 * Key (Integer): sensorID
	 * Value (Integer, <Double, Integer>): sensorID, <sensorValue.sum, sensor.count>
	 * Input (Integer, Double): sensorID, sensorValue
	 * Output (Integer, <Double, Integer>): sensorID, <sensorValue.sum, sensor.count>
	 */
	/*
	private static class SensorValuesSumPreAggregateFunction
		extends PreAggregateFunction<Integer,
		Tuple2<Integer, Tuple2<Double, Integer>>,
		Tuple2<Integer, Double>,
		Tuple2<Integer, Tuple2<Double, Integer>>> {

		@Override
		public Tuple2<Integer, Tuple2<Double, Integer>> addInput(@Nullable Tuple2<Integer, Tuple2<Double, Integer>> value, Tuple2<Integer, Double> input) throws Exception {
			if (value == null) {
				return Tuple2.of(input.f0, Tuple2.of(input.f1, 1));
			} else {
				return Tuple2.of(input.f0, Tuple2.of(value.f1.f0 + input.f1, value.f1.f1 + 1));
			}
		}

		@Override
		public void collect(Map<Integer, Tuple2<Integer, Tuple2<Double, Integer>>> buffer, Collector<Tuple2<Integer, Tuple2<Double, Integer>>> out) throws Exception {
			for (Map.Entry<Integer, Tuple2<Integer, Tuple2<Double, Integer>>> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), Tuple2.of(entry.getValue().f1.f0, entry.getValue().f1.f1)));
			}
		}
	}

	private static class SensorValuesSumPreAggregateConcurrentFunction
		extends PreAggregateConcurrentFunction<Integer,
		Tuple2<Integer, Tuple2<Double, Integer>>,
		Tuple2<Integer, Double>,
		Tuple2<Integer, Tuple2<Double, Integer>>> {

		@Override
		public Tuple2<Integer, Tuple2<Double, Integer>> addInput(@Nullable Tuple2<Integer, Tuple2<Double, Integer>> value, Tuple2<Integer, Double> input) throws Exception {
			if (value == null) {
				return Tuple2.of(input.f0, Tuple2.of(input.f1, 1));
			} else {
				return Tuple2.of(input.f0, Tuple2.of(value.f1.f0 + input.f1, value.f1.f1 + 1));
			}
		}

		@Override
		public void collect(ConcurrentMap<Integer, Tuple2<Integer, Tuple2<Double, Integer>>> buffer, Collector<Tuple2<Integer, Tuple2<Double, Integer>>> out) throws Exception {
			for (Map.Entry<Integer, Tuple2<Integer, Tuple2<Double, Integer>>> entry : buffer.entrySet()) {
				out.collect(Tuple2.of(entry.getKey(), Tuple2.of(entry.getValue().f1.f0, entry.getValue().f1.f1)));
			}
		}
	}

	private static class AverageReduceFunction implements ReduceFunction<Tuple2<Integer, Tuple2<Double, Integer>>> {

		@Override
		public Tuple2<Integer, Tuple2<Double, Integer>> reduce(Tuple2<Integer, Tuple2<Double, Integer>> value1, Tuple2<Integer, Tuple2<Double, Integer>> value2) throws Exception {
			Double sum = value1.f1.f0 + value2.f1.f0;
			Integer qtd = value1.f1.f1 + value2.f1.f1;
			Double average = sum / qtd;
			return Tuple2.of(value1.f0, Tuple2.of(average, 1));
		}
	}

	private static class FlatOutputMap implements MapFunction<Tuple2<Integer, Tuple2<Double, Integer>>, String> {
		@Override
		public String map(Tuple2<Integer, Tuple2<Double, Integer>> value) throws Exception {
			return "SensorID[" + value.f0 + "] qtd[" + value.f1.f1 + "] average[" + value.f1.f0 + "]";
		}
	}
	 */
}
