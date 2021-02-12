package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideTableOutputMap implements MapFunction<Tuple2<Boolean, Tuple2<Long, Long>>, String> {

	@Override
	public String map(Tuple2<Boolean, Tuple2<Long, Long>> value) throws Exception {
		return value.f0 + "|taxi driver: " + value.f1.f0 + "| count: " + value.f1.f1;
	}
}
