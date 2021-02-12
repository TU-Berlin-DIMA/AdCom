package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.udfs.MqttDataSink;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiDriverSumCntKeySelector;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideAveragePassengersReducer;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideAvgPassengerOutputMap;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRidePassengerSumAndCountPreAggregateFunction;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRidePassengerTokenizerMap;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideSource;
import org.apache.flink.streaming.examples.aggregate.udfs.TaxiRideSourceParallel;
import org.apache.flink.streaming.examples.aggregate.util.GenericParameters;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_FLAT_OUTPUT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_PRE_AGGREGATE;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_REDUCER;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_SINK;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_SOURCE;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.OPERATOR_TOKENIZER;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SINK_DATA_MQTT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SINK_TEXT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SLOT_GROUP_01;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SLOT_GROUP_02;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.SLOT_GROUP_DEFAULT;
import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.TOPIC_DATA_SINK;

public class TaxiRideAvgPassengerPreAggregate {
	public static void main(String[] args) throws Exception {
		// @formatter:off
		GenericParameters genericParam = new GenericParameters(args);
		genericParam.printParameters();

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (genericParam.isDisableOperatorChaining()) {
			env.disableOperatorChaining();
		}
		String slotGroup01 = SLOT_GROUP_DEFAULT;
		String slotGroup02 = SLOT_GROUP_DEFAULT;
		if (genericParam.getSlotSplit() == 0) {
			slotGroup01 = SLOT_GROUP_DEFAULT;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (genericParam.getSlotSplit() == 1) {
			slotGroup01 = SLOT_GROUP_01;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (genericParam.getSlotSplit() == 2) {
			slotGroup01 = SLOT_GROUP_01;
			slotGroup02 = SLOT_GROUP_02;
		}

		DataStream<TaxiRide> rides = null;
		if (genericParam.isParallelSource()) {
			rides = env.addSource(new TaxiRideSourceParallel()).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		} else {
			rides = env.addSource(new TaxiRideSource()).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		}

		DataStream<Tuple3<Long, Double, Long>> tuples = rides.map(new TaxiRidePassengerTokenizerMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		DataStream<Tuple3<Long, Double, Long>> preAggregatedStream = null;
		PreAggregateFunction<Long, Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>>
			taxiRideSumAndCountPreAgg = new TaxiRidePassengerSumAndCountPreAggregateFunction();
		if (!genericParam.isEnableController() && genericParam.getPreAggregationProcessingTimer() == -1) {
			// no combiner
			preAggregatedStream = tuples;
		} else if (!genericParam.isEnableController() && genericParam.getPreAggregationProcessingTimer() > 0) {
			// static combiner based on timeout
			preAggregatedStream = tuples.combine(taxiRideSumAndCountPreAgg, genericParam.getPreAggregationProcessingTimer()).name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		} else if (genericParam.isEnableController()) {
			// dynamic combiner with PI controller
			 preAggregatedStream = tuples.adCombine(taxiRideSumAndCountPreAgg, genericParam.getPreAggregationProcessingTimer()).name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		}

		KeyedStream<Tuple3<Long, Double, Long>, Long> keyedByTaxiRider = preAggregatedStream.keyBy(new TaxiDriverSumCntKeySelector());

		DataStream<Tuple3<Long, Double, Long>> averagePassengers = keyedByTaxiRider.reduce(new TaxiRideAveragePassengersReducer()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());

		if (genericParam.getOutput().equalsIgnoreCase(SINK_DATA_MQTT)) {
			averagePassengers
				.map(new TaxiRideAvgPassengerOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02())
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, genericParam.getSinkHost(), genericParam.getSinkPort())).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());
		} else if (genericParam.getOutput().equalsIgnoreCase(SINK_TEXT)) {
			averagePassengers
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());
		} else {
			System.out.println("discarding output");
		}
		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideAvgPassengerPreAggregate.class.getSimpleName());
		// @formatter:on
	}
}
