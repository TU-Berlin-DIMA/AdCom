package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.examples.aggregate.WordCountPreAggregateData;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class DataRateVariationSource extends RichSourceFunction<String> {

	private static final long DEFAULT_INTERVAL_CHANGE_DATA_SOURCE = Time.minutes(5).toMilliseconds();
	private int currentDataSourceId;
	private volatile boolean running = true;
	private long startTime;
	private long milliseconds;

	public DataRateVariationSource() {
		this(100);
	}

	public DataRateVariationSource(long milliseconds) {
		this.startTime = Calendar.getInstance().getTimeInMillis();
		this.currentDataSourceId = 0;
		this.milliseconds = milliseconds;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running) {
			List<String> sourceList = getDataSourceStream();
			for (String line : sourceList) {
				ctx.collect(line);
			}
			Thread.sleep(this.milliseconds);
		}
	}

	private List<String> getDataSourceStream() {
		List<String> currentDataSource = null;
		long elapsedTime = Calendar.getInstance().getTimeInMillis() - DEFAULT_INTERVAL_CHANGE_DATA_SOURCE;
		if (elapsedTime >= startTime) {
			startTime = Calendar.getInstance().getTimeInMillis();

			if (currentDataSourceId == 0 || currentDataSourceId == 4) {
				this.currentDataSourceId = 1;
			} else if (currentDataSourceId == 1) {
				this.currentDataSourceId = 2;
			} else if (currentDataSourceId == 2) {
				this.currentDataSourceId = 3;
			} else if (currentDataSourceId == 3) {
				this.currentDataSourceId = 4;
			}
			String msg = "Changed source file [" + currentDataSourceId + "]";
			System.out.println(msg);
		}
		if (currentDataSourceId == 0 || currentDataSourceId == 4) {
			currentDataSource = Arrays.asList(WordCountPreAggregateData.WORDS_SKEW_01);
		} else if (currentDataSourceId == 1) {
			currentDataSource = Arrays.asList(WordCountPreAggregateData.WORDS_SKEW_02);
		} else if (currentDataSourceId == 2) {
			currentDataSource = Arrays.asList(WordCountPreAggregateData.WORDS_SKEW_03);
		} else if (currentDataSourceId == 3) {
			currentDataSource = Arrays.asList(WordCountPreAggregateData.WORDS_SKEW_04);
		}
		return currentDataSource;
	}

	@Override
	public void cancel() {
		running = false;
	}
}
