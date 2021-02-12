package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideFlatOutputMap implements MapFunction<Tuple2<Long, Long>, String> {
	@Override
	public String map(Tuple2<Long, Long> value) {
		return value.f0 + " - " + value.f1;
	}
}
