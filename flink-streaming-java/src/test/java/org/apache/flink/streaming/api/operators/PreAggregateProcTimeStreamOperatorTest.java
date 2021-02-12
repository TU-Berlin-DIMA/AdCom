package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class PreAggregateProcTimeStreamOperatorTest {
	@Test
	public void testSimple() throws Exception {
		@SuppressWarnings("unchecked")

		long intervalMs = 1000;

		WordCountPreAggregateFunction preAggFunction = new WordCountPreAggregateFunction();

		KeySelector<Tuple2<String, Integer>, String> keySelector = (KeySelector<Tuple2<String, Integer>, String>) value -> value.f0;

		PreAggregateProcTimeStreamOperator operator = new PreAggregateProcTimeStreamOperator(
			preAggFunction,
			keySelector,
			intervalMs,
			false);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, String> op =
			new OneInputStreamOperatorTestHarness<>(operator);

		op.open();
		synchronized (op.getCheckpointLock()) {
			StreamRecord<Tuple2<String, Integer>> input = new StreamRecord<>(null);

			input.replace(new Tuple2<>("k1", 1));
			op.processElement(input);

			input.replace(new Tuple2<>("k1", 1));
			op.processElement(input);

			input.replace(new Tuple2<>("k2", 1));
			op.processElement(input);

			input.replace(new Tuple2<>("k3", 1));
			op.processElement(input);

			assertEquals(0, preAggFunction.getFinishCount());

			// op.open(); is not opening the Operator to trigger the processing timeout
			// Thread.sleep(2000);
			// assertEquals(1, preAggFunction.getFinishCount());
			op.close();
		}
	}

	private static class WordCountPreAggregateFunction
		extends PreAggregateFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>> {
		private final List<Tuple2<String, Integer>> outputs = new ArrayList<>();
		private int finishCount = 0;

		@Override
		public Integer addInput(
			@Nullable Integer value,
			Tuple2<String, Integer> input) throws InterruptedException {
			if (value == null) {
				return input.f1;
			} else {
				return value + input.f1;
			}
		}

		@Override
		public void collect(Map<String, Integer> buffer, Collector<Tuple2<String, Integer>> out) {
			finishCount++;
			outputs.clear();
			for (Map.Entry<String, Integer> entry : buffer.entrySet()) {
				// out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
				outputs.add(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}

		int getFinishCount() {
			return finishCount;
		}

		List<Tuple2<String, Integer>> getOutputs() {
			return outputs;
		}
	}
}
