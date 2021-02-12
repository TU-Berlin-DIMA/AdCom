package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class TaxiRideDateKeySelector implements KeySelector<Tuple3<Integer, Long, Long>, Integer> {

	@Override
	public Integer getKey(Tuple3<Integer, Long, Long> value) throws Exception {
		return value.f0;
	}
}
