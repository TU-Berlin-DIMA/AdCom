package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple12;

import io.airlift.tpch.LineItem;

public class LineItemToTupleTableMap implements MapFunction<LineItem,
	Tuple12<String, String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple12<String, String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> map(
		LineItem lineItem) throws Exception {

		String key = lineItem.getReturnFlag() + "|" + lineItem.getStatus();
		return Tuple12.of(
			key,
			lineItem.getReturnFlag(),
			lineItem.getStatus(),
			lineItem.getQuantity(),
			lineItem.getExtendedPrice(),
			lineItem.getDiscount(),
			lineItem.getExtendedPrice() * (1 - lineItem.getDiscount()),
			lineItem.getExtendedPrice() * (1 - lineItem.getDiscount()) * (1 + lineItem.getTax()),
			lineItem.getQuantity(),
			lineItem.getExtendedPrice(),
			lineItem.getDiscount(),
			1L);
	}
}
