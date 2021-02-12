package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Count the number of values and sum them.
 * Key (Integer): random-key
 * Value (Integer, Double, Long): random-key, passengerCnt.sum, random-key.count
 * Input (Integer, Double): random-key, passengerCnt
 * Output (Integer, Double, Long): random-key, passengerCnt.sum, random-key.count
 */
public class TaxiRidePassengerSumPreAggregateFunction
	extends PreAggregateFunction<Integer,
	Tuple2<Integer, Tuple2<Double, Long>>,
	Tuple2<Integer, Double>,
	Tuple2<Integer, Tuple2<Double, Long>>> {

	@Override
	public Tuple2<Integer, Tuple2<Double, Long>> addInput(
		@Nullable Tuple2<Integer, Tuple2<Double, Long>> value,
		Tuple2<Integer, Double> input) throws Exception {
		Integer randomKey = input.f0;
		if (value == null) {
			Double distance = input.f1;
			return Tuple2.of(randomKey, Tuple2.of(distance, 1L));
		} else {
			Double distance = input.f1 + value.f1.f0;
			Long driverIdCount = value.f1.f1 + 1;
			return Tuple2.of(randomKey, Tuple2.of(distance, driverIdCount));
		}
	}

	@Override
	public void collect(
		Map<Integer, Tuple2<Integer, Tuple2<Double, Long>>> buffer,
		Collector<Tuple2<Integer, Tuple2<Double, Long>>> out) throws Exception {
		for (Map.Entry<Integer, Tuple2<Integer, Tuple2<Double, Long>>> entry : buffer.entrySet()) {
			Double distanceSum = entry.getValue().f1.f0;
			Long driverIdCount = entry.getValue().f1.f1;
			out.collect(Tuple2.of(entry.getKey(), Tuple2.of(distanceSum, driverIdCount)));
		}
	}
}
