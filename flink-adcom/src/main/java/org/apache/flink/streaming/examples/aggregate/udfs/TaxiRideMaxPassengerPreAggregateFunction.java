package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

public class TaxiRideMaxPassengerPreAggregateFunction extends PreAggregateFunction<Long, Long, Tuple2<Long, Long>, Tuple2<Long, Long>> {

	@Override
	public Long addInput(@Nullable Long value, Tuple2<Long, Long> input) throws Exception {
		if (value == null) {
			return input.f1;
		} else {
			if (input.f1 > value) return input.f1;
			else return value;
		}
	}

	@Override
	public void collect(Map<Long, Long> buffer, Collector<Tuple2<Long, Long>> out) {
		for (Map.Entry<Long, Long> entry : buffer.entrySet()) {
			out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
		}
	}
}
