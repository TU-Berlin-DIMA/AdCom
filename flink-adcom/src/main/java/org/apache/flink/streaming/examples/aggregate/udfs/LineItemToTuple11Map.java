package org.apache.flink.streaming.examples.aggregate.udfs;

import io.airlift.tpch.LineItem;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;

public class LineItemToTuple11Map implements MapFunction<LineItem,
	Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String,
		Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> map(
		LineItem lineItem) throws Exception {

		String key = lineItem.getReturnFlag() + "|" + lineItem.getStatus();
		return Tuple2.of(key, Tuple11.of(
			lineItem.getReturnFlag(), lineItem.getStatus(),
			lineItem.getQuantity(),
			lineItem.getExtendedPrice(),
			lineItem.getDiscount(),
			lineItem.getExtendedPrice() * (1 - lineItem.getDiscount()),
			lineItem.getExtendedPrice() * (1 - lineItem.getDiscount()) * (1
				+ lineItem.getTax()),
			lineItem.getQuantity(),
			lineItem.getExtendedPrice(),
			lineItem.getDiscount(),
			1L));
	}
}
