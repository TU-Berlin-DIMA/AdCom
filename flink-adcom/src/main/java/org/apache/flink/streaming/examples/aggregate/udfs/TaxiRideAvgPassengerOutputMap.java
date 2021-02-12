package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class TaxiRideAvgPassengerOutputMap implements MapFunction<Tuple3<Long, Double, Long>, String> {
	@Override
	public String map(Tuple3<Long, Double, Long> value) {
		return value.f0 + " - " + value.f1 + " - " + value.f2;
	}
}
