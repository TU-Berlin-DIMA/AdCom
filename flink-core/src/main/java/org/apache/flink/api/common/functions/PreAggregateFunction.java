package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * @param <K> The key to pre-aggregate elements.
 * @param <V> The value to pre-aggregate elements.
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 */
@Public
public abstract class PreAggregateFunction<K, V, IN, OUT> implements Function {
	private static final long serialVersionUID = 1L;

	/**
	 * Adds the given input to the given value, returning the new bundle value.
	 *
	 * @param value the existing bundle value, maybe null
	 * @param input the given input, not null
	 *
	 * @throws Exception
	 */
	public abstract V addInput(@Nullable V value, IN input) throws Exception;

	/**
	 * Called when a merge is finished. Transform a bundle to zero, one, or more
	 * output elements.
	 */
	public abstract void collect(Map<K, V> buffer, Collector<OUT> out) throws Exception;

	public void close() throws Exception {
	}
}
