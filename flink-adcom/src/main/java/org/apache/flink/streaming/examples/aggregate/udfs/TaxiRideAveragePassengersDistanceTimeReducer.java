package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class TaxiRideAveragePassengersDistanceTimeReducer implements ReduceFunction<Tuple5<Long, Double, Double, Double, Long>> {
	@Override
	public Tuple5<Long, Double, Double, Double, Long> reduce(
		Tuple5<Long, Double, Double, Double, Long> value1,
		Tuple5<Long, Double, Double, Double, Long> value2) throws Exception {
		return Tuple5.of(
			value1.f0,
			(value1.f1 + value2.f1) / (value1.f4 + value2.f4),
			(value1.f2 + value2.f2) / (value1.f4 + value2.f4),
			(value1.f3 + value2.f3) / (value1.f4 + value2.f4),
			1L);
	}
}
