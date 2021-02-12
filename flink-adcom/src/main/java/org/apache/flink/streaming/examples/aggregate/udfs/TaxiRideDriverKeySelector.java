package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideDriverKeySelector implements KeySelector<Tuple2<Integer, Tuple2<Double, Long>>, Integer> {
	@Override
	public Integer getKey(Tuple2<Integer, Tuple2<Double, Long>> value) throws Exception {
		return value.f0;
	}
}

