package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;

public class DataRateSource extends RichSourceFunction<String> {
	private volatile boolean running = true;
	private List<String> currentDataSource = null;
	private long milliseconds;

	public DataRateSource(String[] dataSource) {
		this(dataSource, 1);
	}

	public DataRateSource(String[] dataSource, long milliseconds) {
		this.currentDataSource = Arrays.asList(dataSource);
		this.milliseconds = milliseconds;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Normal Distribution");
		NormalDistribution normalDistribution = new NormalDistribution(10, 3);
		for (int i = 0; i < 10; i++) {
			System.out.println(normalDistribution.sample());
		}

		System.out.println("Zipf Distribution");
		ZipfDistribution zipfDistribution = new ZipfDistribution(10, 3);
		for (int i = 0; i < 10; i++) {
			System.out.println(zipfDistribution.sample());
		}
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running) {
			for (String line : this.currentDataSource) {
				ctx.collect(line);
			}
			if (this.milliseconds != 0) {
				Thread.sleep(this.milliseconds);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
