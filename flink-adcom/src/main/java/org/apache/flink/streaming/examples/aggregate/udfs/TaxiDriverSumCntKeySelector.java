package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class TaxiDriverSumCntKeySelector implements KeySelector<Tuple3<Long, Double, Long>, Long> {

	@Override
	public Long getKey(Tuple3<Long, Double, Long> value) throws Exception {
		return value.f0;
	}
}
