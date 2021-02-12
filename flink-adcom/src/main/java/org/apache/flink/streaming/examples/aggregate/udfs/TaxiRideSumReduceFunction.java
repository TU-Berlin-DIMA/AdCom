package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideSumReduceFunction implements ReduceFunction<Tuple2<Long, Long>> {
	@Override
	public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
		return Tuple2.of(value1.f0, value1.f1 + value2.f1);
	}
}
