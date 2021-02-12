package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

public class TaxiRidePassengerSumAndCountPreAggregateFunction
	extends PreAggregateFunction<Long,
	Tuple3<Long, Double, Long>,
	Tuple3<Long, Double, Long>,
	Tuple3<Long, Double, Long>> {

	@Override
	public Tuple3<Long, Double, Long> addInput(
		@Nullable Tuple3<Long, Double, Long> value,
		Tuple3<Long, Double, Long> input) throws Exception {
		Long key = input.f0;
		if (value == null) {
			return Tuple3.of(key, input.f1, input.f2);
		} else {
			return Tuple3.of(key, value.f1 + input.f1, value.f2 + input.f2);
		}
	}

	@Override
	public void collect(
		Map<Long, Tuple3<Long, Double, Long>> buffer,
		Collector<Tuple3<Long, Double, Long>> out) throws Exception {
		for (Map.Entry<Long, Tuple3<Long, Double, Long>> entry : buffer.entrySet()) {
			out.collect(Tuple3.of(
				entry.getKey(),
				entry.getValue().f1,
				entry.getValue().f2));
		}
	}
}
