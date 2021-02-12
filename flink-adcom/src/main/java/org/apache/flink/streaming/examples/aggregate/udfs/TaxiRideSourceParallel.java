package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.examples.aggregate.util.DataRateListener;
import org.apache.flink.streaming.examples.aggregate.util.ExerciseBase;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;

public class TaxiRideSourceParallel extends RichParallelSourceFunction<TaxiRide> {

	public static final String WORKLOAD_FILE = "/tmp/workloadFile.txt";
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss");
	private final int maxDelayMsecs;
	private final int watermarkDelayMSecs;

	private String dataFilePath;
	private DataRateListener dataRateListener;
	private boolean running;
	private transient BufferedReader reader;
	private transient InputStream gzipStream;

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * at the speed at which they were originally generated.
	 */
	public TaxiRideSourceParallel() {
		this(0);
	}

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served out-of time stamp order with specified maximum random delay
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param maxEventDelaySecs The max time in seconds by which events are delayed.
	 */
	public TaxiRideSourceParallel(int maxEventDelaySecs) {
		if (maxEventDelaySecs < 0) {
			throw new IllegalArgumentException("Max event delay must be positive");
		}
		this.running = true;
		this.maxDelayMsecs = maxEventDelaySecs * 1000;
		this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
	}

	public static void main(String[] args) {
		TaxiRideSourceParallel source = new TaxiRideSourceParallel();
		source.changeWorkloadFile();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.dataRateListener = new DataRateListener();
		this.dataRateListener.start();
	}

	@Override
	public void run(SourceContext<TaxiRide> sourceContext) throws Exception {
		while (running) {
			// check if the workload file changed and apply the new file if it is necessary
			changeWorkloadFile();

			generateTaxiRideArray(sourceContext);
		}
	}

	private void changeWorkloadFile() {
		File fileName = new File(WORKLOAD_FILE);
		try (BufferedReader br = new BufferedReader(new InputStreamReader(
			new FileInputStream(fileName), StandardCharsets.UTF_8))) {

			String line;
			if ((line = br.readLine()) != null) {
				// System.out.println(line);
				this.dataFilePath = line;
				System.out.println(
					"[" + sdf.format(new Date()) + "] Reading workload file [" + this.dataFilePath
						+ "]");
			} else {
				System.out.println("[" + sdf.format(new Date()) + "] File [" + WORKLOAD_FILE
					+ "] is empty. reading the original workload [" + ExerciseBase.pathToRideData
					+ "]");
				this.dataFilePath = ExerciseBase.pathToRideData;
			}

		} catch (FileNotFoundException e) {
			System.out.println("File does not [" + WORKLOAD_FILE
				+ "] exists. Reading the original workload ");
			this.dataFilePath = ExerciseBase.pathToRideData;
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("File does not [" + WORKLOAD_FILE
				+ "] exists. Reading the original workload ");
			this.dataFilePath = ExerciseBase.pathToRideData;
			e.printStackTrace();
		}
	}

	private void generateTaxiRideArray(SourceContext<TaxiRide> sourceContext) throws Exception {
		gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
		reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));
		String line;
		TaxiRide taxiRide;
		long startTime;
		while (reader.ready() && (line = reader.readLine()) != null) {
			startTime = System.nanoTime();
			taxiRide = TaxiRide.fromString(line);

			sourceContext.collectWithTimestamp(taxiRide, getEventTime(taxiRide));

			// sleep in nanoseconds to have a reproducible data rate for the data source
			this.dataRateListener.busySleep(startTime);
		}

		this.reader.close();
		this.reader = null;
		this.gzipStream.close();
		this.gzipStream = null;
	}

	public long getEventTime(TaxiRide ride) {
		return ride.getEventTime();
	}

	@Override
	public void cancel() {
		try {
			this.running = false;
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.gzipStream != null) {
				this.gzipStream.close();
			}
		} catch (IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.gzipStream = null;
		}
	}
}
