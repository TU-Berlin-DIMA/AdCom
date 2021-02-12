package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class TaxiRideAvgTableOutputMap implements MapFunction<Tuple2<Boolean, Tuple4<Long, Long, Double, Double>>, String> {

	@Override
	public String map(Tuple2<Boolean, Tuple4<Long, Long, Double, Double>> value) throws Exception {
		return value.f0 +
			"|taxi driver: " + value.f1.f0 +
			"| passengers: " + value.f1.f1 +
			"| distance: " + value.f1.f2 +
			"| elapseTime: " + value.f1.f3;
	}
}
