package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

public class LineItemSumPreAgg extends PreAggregateFunction<String,
	Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>,
	Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>,
	Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> addInput(
		@Nullable Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value,
		Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> input) throws Exception {
		if (value == null) {
			return input.f1;
		} else {
			String flag = input.f1.f0;
			String status = input.f1.f1;
			Long sumQty = input.f1.f2 + value.f2;
			Double sumBasePrice = input.f1.f3 + value.f3;
			Double sumDisc = input.f1.f4 + value.f4;
			Double sumDiscPrice = input.f1.f5 + value.f5;
			Double sumCharge = input.f1.f6 + value.f6;
			Long avgQty = 0L;
			Double avgBasePrice = 0.0;
			Double avgDisc = 0.0;
			Long count = input.f1.f10 + value.f10;

			return Tuple11.of(
				flag,
				status,
				sumQty,
				sumBasePrice,
				sumDisc,
				sumDiscPrice,
				sumCharge,
				avgQty,
				avgBasePrice,
				avgDisc,
				count);
		}
	}

	@Override
	public void collect(
		Map<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> buffer,
		Collector<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> out) throws Exception {
		for (Map.Entry<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> entry : buffer
			.entrySet()) {
			out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
		}
	}
}
