package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;

public class LineItemFlagAndStatusKeySelector implements KeySelector<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>, String> {
	private static final long serialVersionUID = 1L;

	@Override
	public String getKey(Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> value) throws Exception {
		return value.f0;
	}
}
