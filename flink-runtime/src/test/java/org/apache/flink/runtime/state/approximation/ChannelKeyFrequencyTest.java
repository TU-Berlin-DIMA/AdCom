package org.apache.flink.runtime.state.approximation;

import org.junit.Assert;
import org.junit.Test;

public class ChannelKeyFrequencyTest {

	@Test
	public void testHighestFrequency() {
		ChannelKeyFrequency channelKeyFrequency = new ChannelKeyFrequency(4, 10);
		channelKeyFrequency.add(5, 0);
		channelKeyFrequency.add(10, 1);
		channelKeyFrequency.add(20, 2);
		channelKeyFrequency.add(30, 3);

		channelKeyFrequency.add(5, 0);
		channelKeyFrequency.add(10, 1);

		channelKeyFrequency.add(5, 0);

		Assert.assertEquals(3, channelKeyFrequency.getHighestFrequency());
	}

	@Test
	public void testFactorBetweenHighestFrequencies() {
		ChannelKeyFrequency channelKeyFrequency = new ChannelKeyFrequency(4, 10);

		for (int i = 0; i < 10; i++) {
			channelKeyFrequency.add(5, 0);
		}
		Assert.assertEquals(10, channelKeyFrequency.getHighestFrequency());

		for (int i = 0; i < 100; i++) {
			channelKeyFrequency.add(10, 1);
		}
		Assert.assertEquals(100, channelKeyFrequency.getHighestFrequency());

		Assert.assertEquals(10, channelKeyFrequency.getFactorBetweenHighestFrequencies());
	}

	@Test
	public void testFactorBetweenHighestFrequenciesOpposite() {
		ChannelKeyFrequency channelKeyFrequency = new ChannelKeyFrequency(4, 10);

		for (int i = 0; i < 100; i++) {
			channelKeyFrequency.add(10, 1);
		}
		Assert.assertEquals(100, channelKeyFrequency.getHighestFrequency());

		for (int i = 0; i < 10; i++) {
			channelKeyFrequency.add(5, 0);
		}
		Assert.assertEquals(100, channelKeyFrequency.getHighestFrequency());

		Assert.assertEquals(10, channelKeyFrequency.getFactorBetweenHighestFrequencies());
	}

	@Test
	public void testNumberOfHops() {
		ChannelKeyFrequency channelKeyFrequency = new ChannelKeyFrequency(4, 10);

		for (int i = 0; i < 10; i++) {
			channelKeyFrequency.add(5, 0);
		}
		Assert.assertEquals(10, channelKeyFrequency.getHighestFrequency());
		Assert.assertEquals(0, channelKeyFrequency.getNumberOfHops());

		channelKeyFrequency.add(10, 1);
		Assert.assertEquals(10, channelKeyFrequency.getHighestFrequency());
		Assert.assertEquals(1, channelKeyFrequency.getNumberOfHops());

		for (int i = 0; i < 99; i++) {
			channelKeyFrequency.add(10, 1);
		}
		Assert.assertEquals(100, channelKeyFrequency.getHighestFrequency());
	}
}
