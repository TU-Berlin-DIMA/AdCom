package org.apache.flink.streaming.examples.aggregate.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class DataRateListener extends Thread implements Serializable {

	public static final String DATA_RATE_FILE = "/tmp/datarate.txt";
	private long delayInNanoSeconds;
	private boolean running;

	public DataRateListener() {
		// 1 millisecond = 1.000.000 nanoseconds
		// 1.000.000.000 = 1 second
		// 1.000.000.000 / 1.000.000 = 1.000 records/second
		this.delayInNanoSeconds = 1000000000;
		this.running = true;
		this.disclaimer();
	}

	public static void main(String[] args) {
		DataRateListener drl = new DataRateListener();
		long start;
		System.out.println("delay                        : " + drl.delayInNanoSeconds);
		for (int i = 0; i < 100; i++) {
			start = System.nanoTime();
			System.out.print("start : " + start);
			drl.busySleep(start);
			System.out.println(" finish: " + (System.nanoTime() - start));
		}
	}

	private void disclaimer() {
		// @formatter:off
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] class to read data rate from file [" + DATA_RATE_FILE + "] in milliseconds.");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] This listener reads every 60 seconds only the first line from the data rate file.");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] Use the following command to change the millisecond data rate:");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '1000000000' > /tmp/datarate.txt    # 1    rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '1000000' > /tmp/datarate.txt       # 1K   rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '200000' > /tmp/datarate.txt        # 5K   rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '100000' > /tmp/datarate.txt        # 10K  rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '66666' > /tmp/datarate.txt         # 15K  rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '50000' > /tmp/datarate.txt         # 20K  rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '20000' > /tmp/datarate.txt         # 50K  rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '10000' > /tmp/datarate.txt         # 100K rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '5000' > /tmp/datarate.txt          # 200K rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '2000' > /tmp/datarate.txt          # 500K rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '1000' > /tmp/datarate.txt          # 1M   rec/sec");
		System.out.println("[" + DataRateListener.class.getSimpleName() + "] echo '500' > /tmp/datarate.txt           # 2M   rec/sec");
		System.out.println();
		// @formatter:on
	}

	public void run() {
		while (running) {
			File fileName = new File(DATA_RATE_FILE);
			try (BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(fileName), StandardCharsets.UTF_8))) {

				String line;
				while ((line = br.readLine()) != null) {
					// System.out.println(line);
					if (isNumeric(line)) {
						if (Long.parseLong(line) > 0) {
							long millisec = Math.round(1_000_000_000 / Long.parseLong(line));
							System.out.println("[DataRateListener] Reading [" + line
								+ "] new frequency to generate data: " + millisec
								+ " rec/sec.");
							delayInNanoSeconds = Long.parseLong(line);
						} else {
							System.out.println(
								"[DataRateListener] ERROR: new frequency must be greater or equal to 1.");
						}
					} else if ("SHUTDOWN".equalsIgnoreCase(line)) {
						running = false;
					} else {
						System.out.println(
							"[DataRateListener] ERROR: new frequency must be a number. But it is: "
								+ line);
					}
				}
				Thread.sleep(60 * 1000);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public long getDelayInNanoSeconds() {
		return this.delayInNanoSeconds;
	}

	public void busySleep(long startTime) {
		long deadLine = startTime + this.delayInNanoSeconds;
		while (System.nanoTime() < deadLine) ;
	}

	public boolean isNumeric(final String str) {
		// null or empty
		if (str == null || str.length() == 0) {
			return false;
		}
		for (char c : str.toCharArray()) {
			if (!Character.isDigit(c)) {
				return false;
			}
		}
		return true;
	}
}
