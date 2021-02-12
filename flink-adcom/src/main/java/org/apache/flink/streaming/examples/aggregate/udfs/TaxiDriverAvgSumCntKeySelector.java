package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;

public class TaxiDriverAvgSumCntKeySelector implements KeySelector<Tuple5<Long, Double, Double, Double, Long>, Long> {

	@Override
	public Long getKey(Tuple5<Long, Double, Double, Double, Long> value) throws Exception {
		return value.f0;
	}
}
