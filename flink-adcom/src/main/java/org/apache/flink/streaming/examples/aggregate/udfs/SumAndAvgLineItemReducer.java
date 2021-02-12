package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;

public class SumAndAvgLineItemReducer implements ReduceFunction<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> reduce(
		Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> value1,
		Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> value2) throws Exception {

		String key = value1.f0;
		Long count = value1.f1.f10 + value2.f1.f10;
		Long sumQty = value1.f1.f2 + value2.f1.f2;
		Double sumBasePrice = value1.f1.f3 + value2.f1.f3;
		Double sumDisc = value1.f1.f4 + value2.f1.f4;
		Double sumDiscPrice = value1.f1.f5 + value2.f1.f5;
		Double sumCharge = value1.f1.f6 + value2.f1.f6;
		Long avgQty = sumQty / count;
		Double avgBasePrice = sumBasePrice / count;
		Double avgDisc = sumDisc / count;

		return Tuple2.of(
			key,
			Tuple11.of(value1.f1.f0, value1.f1.f1,
				sumQty,
				sumBasePrice,
				sumDisc,
				sumDiscPrice,
				sumCharge,
				avgQty,
				avgBasePrice,
				avgDisc,
				count));
	}
}
