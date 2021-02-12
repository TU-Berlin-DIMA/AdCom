package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

public class TaxiRideCountDistinctPreAggregateFunction extends PreAggregateFunction<Integer,
	Tuple3<Integer, Long, Long>,
	Tuple3<Integer, Long, Long>,
	Tuple3<Integer, Long, Long>> {
	@Override
	public Tuple3<Integer, Long, Long> addInput(
		@Nullable Tuple3<Integer, Long, Long> value,
		Tuple3<Integer, Long, Long> input) throws Exception {
		if (value == null) {
			return input;
		} else {
			return Tuple3.of(input.f0, input.f1, value.f2 + input.f2);
		}
	}

	@Override
	public void collect(
		Map<Integer, Tuple3<Integer, Long, Long>> buffer,
		Collector<Tuple3<Integer, Long, Long>> out) throws Exception {
		for (Map.Entry<Integer, Tuple3<Integer, Long, Long>> entry : buffer.entrySet()) {
			out.collect(Tuple3.of(entry.getKey(), entry.getValue().f1, entry.getValue().f2));
		}
	}
}
