package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.udfs.*;
import org.apache.flink.streaming.examples.aggregate.util.GenericParameters;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;

public class TaxiRideAveragePreAggregate {
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

		DataStream<Tuple5<Long, Double, Double, Double, Long>> tuples = rides.map(new TaxiRidePassengerDistanceTimeTokenizerMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		DataStream<Tuple5<Long, Double, Double, Double, Long>> preAggregatedStream = null;
		PreAggregateFunction<Long, Tuple5<Long, Double, Double, Double, Long>, Tuple5<Long, Double, Double, Double, Long>, Tuple5<Long, Double, Double, Double, Long>>
			taxiRideSumAndCountPreAgg = new TaxiRidePassengerDistanceTimeSumAndCountPreAggregateFunction();
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

		KeyedStream<Tuple5<Long, Double, Double, Double, Long>, Long> keyedByTaxiRider = preAggregatedStream.keyBy(new TaxiDriverAvgSumCntKeySelector());

		DataStream<Tuple5<Long, Double, Double, Double, Long>> averagePassengers = keyedByTaxiRider.reduce(new TaxiRideAveragePassengersDistanceTimeReducer()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());

		if (genericParam.getOutput().equalsIgnoreCase(SINK_DATA_MQTT)) {
			averagePassengers
				.map(new TaxiRideAvgPassengerDistanceTimeOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02())
				.addSink(new MqttDataSink(TOPIC_DATA_SINK, genericParam.getSinkHost(), genericParam.getSinkPort())).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());
		} else if (genericParam.getOutput().equalsIgnoreCase(SINK_TEXT)) {
			averagePassengers
				.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());
		} else {
			System.out.println("discarding output");
		}
		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TaxiRideAveragePreAggregate.class.getSimpleName());
		// @formatter:on
	}
}
