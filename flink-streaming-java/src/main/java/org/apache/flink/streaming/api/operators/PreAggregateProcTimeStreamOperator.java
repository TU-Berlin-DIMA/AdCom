package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class PreAggregateProcTimeStreamOperator<K, V, IN, OUT> extends PreAggregateProcTimeStreamAbstractOperator<K, V, IN, OUT> {

	private static final long serialVersionUID = 1L;

	/**
	 * KeySelector is used to extract key for bundle map.
	 */
	private final KeySelector<IN, K> keySelector;

	public PreAggregateProcTimeStreamOperator(
		PreAggregateFunction<K, V, IN, OUT> function,
		KeySelector<IN, K> keySelector,
		long intervalMs,
		boolean enableController) {
		super(function, intervalMs, enableController);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}
