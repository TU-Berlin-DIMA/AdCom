package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class TaxiRideAveragePassengersReducer implements ReduceFunction<Tuple3<Long, Double, Long>> {
	@Override
	public Tuple3<Long, Double, Long> reduce(
		Tuple3<Long, Double, Long> value1,
		Tuple3<Long, Double, Long> value2) throws Exception {
		return Tuple3.of(value1.f0, (value1.f1 + value2.f1) / (value1.f2 + value2.f2), 1L);
	}
}
