package org.apache.flink.runtime.state.approximation;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.IFrequency;

public class ChannelKeyFrequency {

	private IFrequency frequency;
	private long[] channelState;
	private long factor;

	public ChannelKeyFrequency(int parallelism, long factor) {
		this(parallelism, factor, 12, 2045, 1);
	}

	public ChannelKeyFrequency(int parallelism, long factor, int depth, int width, int seed) {
		this.frequency = new CountMinSketch(depth, width, seed);
		this.channelState = new long[parallelism];
		this.factor = factor;
	}

	public long getChannelFrequencyState(int position) {
		return this.channelState[position];
	}

	public void add(Object key, int channel) {
		this.frequency.add(key.toString(), 1);
		long keyFrequencyEstimation = estimateCount(key);
		addKeyChannelState(channel, keyFrequencyEstimation);
	}

	public void add(long key, int channel) {
		this.frequency.add(key, 1);
		long keyFrequencyEstimation = estimateCount(key);
		addKeyChannelState(channel, keyFrequencyEstimation);
	}

	public long estimateCount(Object key) {
		return this.frequency.estimateCount(key.toString());
	}

	public long estimateCount(long key) {
		return this.frequency.estimateCount(key);
	}

	private void addKeyChannelState(int channel, long keyFrequencyEstimation) {
		if (keyFrequencyEstimation > channelState[channel]) {
			channelState[channel] = keyFrequencyEstimation;
		}
	}

	public long getHighestFrequency() {
		long firstHighestFrequency = 0;
		for (int i = 0; i < channelState.length; i++) {
			if (channelState[i] > firstHighestFrequency) {
				firstHighestFrequency = channelState[i];
			}
		}
		return firstHighestFrequency;
	}

	public long getFactorBetweenHighestFrequencies() {
		long firstHighestFrequency = 0;
		long secondHighestFrequency = 0;
		for (int i = 0; i < channelState.length; i++) {
			if (channelState[i] > firstHighestFrequency) {
				secondHighestFrequency = firstHighestFrequency;
				firstHighestFrequency = channelState[i];
			} else if (channelState[i] > secondHighestFrequency) {
				secondHighestFrequency = channelState[i];
			}
		}
		if (firstHighestFrequency == 0 || secondHighestFrequency == 0) {
			return 0;
		}
		return firstHighestFrequency / secondHighestFrequency;
	}

	public long getNumberOfHops() {
		long factorBetweenFreq = getFactorBetweenHighestFrequencies();
		// System.err.println("factorBetweenFreq[" + factorBetweenFreq + "]");
		return factorBetweenFreq / factor;
	}
}
