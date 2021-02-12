package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

public class TaxiRidePassengerDistanceTimeSumAndCountPreAggregateFunction
	extends PreAggregateFunction<Long,
	Tuple5<Long, Double, Double, Double, Long>,
	Tuple5<Long, Double, Double, Double, Long>,
	Tuple5<Long, Double, Double, Double, Long>> {

	@Override
	public Tuple5<Long, Double, Double, Double, Long> addInput(
		@Nullable Tuple5<Long, Double, Double, Double, Long> value,
		Tuple5<Long, Double, Double, Double, Long> input) throws Exception {
		Long key = input.f0;
		if (value == null) {
			return Tuple5.of(key, input.f1, input.f2, input.f3, input.f4);
		} else {
			return Tuple5.of(
				key,
				value.f1 + input.f1,
				value.f2 + input.f2,
				value.f3 + input.f3,
				value.f4 + input.f4);
		}
	}

	@Override
	public void collect(
		Map<Long, Tuple5<Long, Double, Double, Double, Long>> buffer,
		Collector<Tuple5<Long, Double, Double, Double, Long>> out) throws Exception {
		for (Map.Entry<Long, Tuple5<Long, Double, Double, Double, Long>> entry : buffer.entrySet()) {
			out.collect(Tuple5.of(
				entry.getKey(),
				entry.getValue().f1,
				entry.getValue().f2,
				entry.getValue().f3,
				entry.getValue().f4));
		}
	}
}
