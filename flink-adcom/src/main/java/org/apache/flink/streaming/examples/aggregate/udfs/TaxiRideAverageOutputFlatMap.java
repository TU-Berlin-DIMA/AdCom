package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideAverageOutputFlatMap implements MapFunction<Tuple2<Integer, Tuple2<Double, Long>>, String> {
	@Override
	public String map(Tuple2<Integer, Tuple2<Double, Long>> value) {
		return "Average distance[" + value.f1.f0 + "] Kilometers";
	}
}
