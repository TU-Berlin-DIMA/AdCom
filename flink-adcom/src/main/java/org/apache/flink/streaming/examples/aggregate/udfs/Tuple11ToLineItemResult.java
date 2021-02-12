package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;

public class Tuple11ToLineItemResult implements MapFunction<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>, String> {
	private static final long serialVersionUID = 1L;

	@Override
	public String map(Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> value) throws Exception {
		String msg = "flag:" + value.f1.f0 +
			" status:" + value.f1.f1 +
			" sum_qty:" + value.f1.f2 +
			" sum_base_price:" + value.f1.f3 +
			" sum_disc:" + value.f1.f4 +
			" sum_disc_price:" + value.f1.f5 +
			" sum_charge:" + value.f1.f6 +
			" avg_qty:" + value.f1.f7 +
			" avg_price:" + value.f1.f8 +
			" avg_disc:" + value.f1.f9 +
			" order_qty:" + value.f1.f10;
		return msg;
	}
}
