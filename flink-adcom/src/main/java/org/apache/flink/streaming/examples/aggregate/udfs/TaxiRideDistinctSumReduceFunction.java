package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class TaxiRideDistinctSumReduceFunction implements ReduceFunction<Tuple3<Integer, Long, Long>> {
	@Override
	public Tuple3<Integer, Long, Long> reduce(
		Tuple3<Integer, Long, Long> value1,
		Tuple3<Integer, Long, Long> value2) {
		return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
	}
}
