package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideTableCountDistinctOutputMap implements MapFunction<Tuple2<Boolean, Tuple2<Integer, Long>>, String> {

	@Override
	public String map(Tuple2<Boolean, Tuple2<Integer, Long>> value) throws Exception {
		return value.f0 + "| dayOfTheYear: " + value.f1.f0 + "| driverId count distinct: "
			+ value.f1.f1;
	}
}
