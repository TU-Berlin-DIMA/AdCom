package org.apache.flink.streaming.util.functions;

import org.apache.flink.metrics.Gauge;

public class PreAggIntervalMsGauge implements Gauge<Long> {

	private long intervalMs;

	@Override
	public Long getValue() {
		return intervalMs;
	}

	public void updateValue(long intervalMs) {
		this.intervalMs = intervalMs;
	}
}
