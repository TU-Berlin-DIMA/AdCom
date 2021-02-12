package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;

public class LineItemTableOutputMap implements MapFunction<Tuple2<Boolean, Tuple10<String, String, Long, Double, Double, Double, Long, Double, Double, Long>>, String> {

	@Override
	public String map(Tuple2<Boolean, Tuple10<String, String, Long, Double, Double, Double, Long, Double, Double, Long>> value) throws Exception {
		return "lineItem: " + value.f0 +
			" | returnflag: " + value.f1.f0 +
			" | linestatus: " + value.f1.f1 +
			" | sum_qty: " + value.f1.f2 +
			" | sum_base_price: " + value.f1.f3 +
			" | sum_disc_price: " + value.f1.f4 +
			" | sum_charge: " + value.f1.f5 +
			" | avg_qty: " + value.f1.f6 +
			" | avg_price: " + value.f1.f7 +
			" | avg_disc: " + value.f1.f8 +
			" | count_order: " + value.f1.f9;
	}
}
