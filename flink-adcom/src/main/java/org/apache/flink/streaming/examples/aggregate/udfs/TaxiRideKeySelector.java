package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaxiRideKeySelector implements KeySelector<Tuple2<Long, Long>, Long> {

	@Override
	public Long getKey(Tuple2<Long, Long> value) throws Exception {
		return value.f0;
	}
}
