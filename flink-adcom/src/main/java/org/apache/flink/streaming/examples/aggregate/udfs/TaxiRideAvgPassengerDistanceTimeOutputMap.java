package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class TaxiRideAvgPassengerDistanceTimeOutputMap implements MapFunction<Tuple5<Long, Double, Double, Double, Long>, String> {
	@Override
	public String map(Tuple5<Long, Double, Double, Double, Long> value) {
		return value.f0 + " - " + value.f1 + " - " + value.f2 + " - " + value.f3 + " - " + value.f4;
	}
}
