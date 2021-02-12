package org.apache.flink.runtime.controller;

public class PreAggregateSignalsState {

	private final int subtaskIndex;
	// Network buffer usage
	private final long[] outPoolUsageMin;
	private final long[] outPoolUsageMax;
	private final double[] outPoolUsageMean;
	private final double[] outPoolUsage05;
	private final double[] outPoolUsage075;
	private final double[] outPoolUsage095;
	private final double[] outPoolUsage099;
	private final double[] outPoolUsageStdDev;
	// Throughput
	private final double[] numRecordsInPerSecond;
	private final double[] numRecordsOutPerSecond;
	// interval in milliseconds
	private final long[] intervalMs;

	public PreAggregateSignalsState(
		String subtaskIndex,
		String outPoolUsageMin,
		String outPoolUsageMax,
		String outPoolUsageMean,
		String outPoolUsage05,
		String outPoolUsage075,
		String outPoolUsage095,
		String outPoolUsage099,
		String outPoolUsageStdDev,
		String numRecordsInPerSecond,
		String numRecordsOutPerSecond,
		String intervalMs) {

		this.subtaskIndex = Integer.parseInt(subtaskIndex);
		// Network buffer usage
		this.outPoolUsageMin = new long[]{Long.parseLong(outPoolUsageMin), -1, -1};
		this.outPoolUsageMax = new long[]{Long.parseLong(outPoolUsageMax), -1, -1};
		this.outPoolUsageMean = new double[]{Double.parseDouble(outPoolUsageMean), -1.0, -1.0};
		this.outPoolUsage05 = new double[]{Double.parseDouble(outPoolUsage05), -1.0, -1.0};
		this.outPoolUsage075 = new double[]{Double.parseDouble(outPoolUsage075), -1.0, -1.0};
		this.outPoolUsage095 = new double[]{Double.parseDouble(outPoolUsage095), -1.0, -1.0};
		this.outPoolUsage099 = new double[]{Double.parseDouble(outPoolUsage099), -1.0, -1.0};
		this.outPoolUsageStdDev = new double[]{Double.parseDouble(outPoolUsageStdDev), -1.0, -1.0};
		// Throughput
		this.numRecordsInPerSecond = new double[]{Double.parseDouble(numRecordsInPerSecond), -1.0, -1.0};
		this.numRecordsOutPerSecond = new double[]{Double.parseDouble(numRecordsOutPerSecond), -1.0, -1.0};
		// Pre-agg intervalMs
		this.intervalMs = new long[]{Long.parseLong(intervalMs), -1, -1};
	}

	public static void main(String[] args) {
		PreAggregateSignalsState p = new PreAggregateSignalsState("0", "1", "1", "1",
			"1", "1", "1", "1", "1",
			"1", "1", "1");
		p.update("0", "1", "1", "1",
			"1", "1", "1", "1", "1",
			"1", "1", "1");
		System.out.println(
			"subtask[" + p.getSubtaskIndex() + "] OutPoolUsageMin[" + p.getOutPoolUsageMin() +
				"] OutPoolUsageMean[" + p.getOutPoolUsageMean() + "]");

		p.update("0", "1", "1", "1",
			"1", "1", "1", "1", "1",
			"1", "1", "1");
		p.update("0", "1", "1", "1",
			"1", "1", "1", "1", "1",
			"1", "1", "1");
		System.out.println(
			"subtask[" + p.getSubtaskIndex() + "] OutPoolUsageMin[" + p.getOutPoolUsageMin() +
				"] OutPoolUsageMean[" + p.getOutPoolUsageMean() + "]");
	}

	public void update(
		String subtaskIndex,
		String outPoolUsageMin,
		String outPoolUsageMax,
		String outPoolUsageMean,
		String outPoolUsage05,
		String outPoolUsage075,
		String outPoolUsage095,
		String outPoolUsage099,
		String outPoolUsageStdDev,
		String numRecordsInPerSecond,
		String numRecordsOutPerSecond,
		String intervalMs) {
		if (this.subtaskIndex != Integer.parseInt(subtaskIndex)) {
			System.out.println(
				"[PreAggregateSignalsState] ERROR: current subtaskIndex[" + subtaskIndex
					+ "] is not equal to the state subtaskIndex[" + this.subtaskIndex + "]");
			return;
		}
		// Network buffer usage
		long outPoolUsageMin01 = this.outPoolUsageMin[0];
		long outPoolUsageMin02 = this.outPoolUsageMin[1];
		this.outPoolUsageMin[0] = Long.parseLong(outPoolUsageMin);
		this.outPoolUsageMin[1] = outPoolUsageMin01;
		this.outPoolUsageMin[2] = outPoolUsageMin02;

		long outPoolUsageMax01 = this.outPoolUsageMax[0];
		long outPoolUsageMax02 = this.outPoolUsageMax[1];
		this.outPoolUsageMax[0] = Long.parseLong(outPoolUsageMax);
		this.outPoolUsageMax[1] = outPoolUsageMax01;
		this.outPoolUsageMax[2] = outPoolUsageMax02;

		double outPoolUsageMean01 = this.outPoolUsageMean[0];
		double outPoolUsageMean02 = this.outPoolUsageMean[1];
		this.outPoolUsageMean[0] = Double.parseDouble(outPoolUsageMean);
		this.outPoolUsageMean[1] = outPoolUsageMean01;
		this.outPoolUsageMean[2] = outPoolUsageMean02;

		double outPoolUsage0501 = this.outPoolUsage05[0];
		double outPoolUsage0502 = this.outPoolUsage05[1];
		this.outPoolUsage05[0] = Double.parseDouble(outPoolUsage05);
		this.outPoolUsage05[1] = outPoolUsage0501;
		this.outPoolUsage05[2] = outPoolUsage0502;

		double outPoolUsage07501 = this.outPoolUsage075[0];
		double outPoolUsage07502 = this.outPoolUsage075[1];
		this.outPoolUsage075[0] = Double.parseDouble(outPoolUsage075);
		this.outPoolUsage075[1] = outPoolUsage07501;
		this.outPoolUsage075[2] = outPoolUsage07502;

		double outPoolUsage09501 = this.outPoolUsage095[0];
		double outPoolUsage09502 = this.outPoolUsage095[1];
		this.outPoolUsage095[0] = Double.parseDouble(outPoolUsage095);
		this.outPoolUsage095[1] = outPoolUsage09501;
		this.outPoolUsage095[2] = outPoolUsage09502;

		double outPoolUsage09901 = this.outPoolUsage099[0];
		double outPoolUsage09902 = this.outPoolUsage099[1];
		this.outPoolUsage099[0] = Double.parseDouble(outPoolUsage099);
		this.outPoolUsage099[1] = outPoolUsage09901;
		this.outPoolUsage099[2] = outPoolUsage09902;

		double outPoolUsageStdDev01 = this.outPoolUsageStdDev[0];
		double outPoolUsageStdDev02 = this.outPoolUsageStdDev[1];
		this.outPoolUsageStdDev[0] = Double.parseDouble(outPoolUsageStdDev);
		this.outPoolUsageStdDev[1] = outPoolUsageStdDev01;
		this.outPoolUsageStdDev[2] = outPoolUsageStdDev02;

		// Throughput
		double numRecordsInPerSecond01 = this.numRecordsInPerSecond[0];
		double numRecordsInPerSecond02 = this.numRecordsInPerSecond[1];
		this.numRecordsInPerSecond[0] = Double.parseDouble(numRecordsInPerSecond);
		this.numRecordsInPerSecond[1] = numRecordsInPerSecond01;
		this.numRecordsInPerSecond[2] = numRecordsInPerSecond02;

		double numRecordsOutPerSecond01 = this.numRecordsOutPerSecond[0];
		double numRecordsOutPerSecond02 = this.numRecordsOutPerSecond[1];
		this.numRecordsOutPerSecond[0] = Double.parseDouble(numRecordsOutPerSecond);
		this.numRecordsOutPerSecond[1] = numRecordsOutPerSecond01;
		this.numRecordsOutPerSecond[2] = numRecordsOutPerSecond02;

		// Pre-agg interval milliseconds
		long intervalMs01 = this.intervalMs[0];
		long intervalMs02 = this.intervalMs[1];
		this.intervalMs[0] = Long.parseLong(intervalMs);
		this.intervalMs[1] = intervalMs01;
		this.intervalMs[2] = intervalMs02;
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	public long getIntervalMs() {
		return intervalMs[0];
	}

	public long getOutPoolUsageMin() {
		return outPoolUsageMin[0];
	}

	public long getOutPoolUsageMinAvg() {
		return average(outPoolUsageMin);
	}

	public long getOutPoolUsageMax() {
		return outPoolUsageMax[0];
	}

	public long getOutPoolUsageMaxAvg() {
		return average(outPoolUsageMax);
	}

	public double getOutPoolUsageMean() {
		return outPoolUsageMean[0];
	}

	public double getOutPoolUsageMeanAvg() {
		return average(outPoolUsageMean);
	}

	public double getOutPoolUsage05() {
		return outPoolUsage05[0];
	}

	public double getOutPoolUsage05Avg() {
		return average(outPoolUsage05);
	}

	public double getOutPoolUsage075() {
		return outPoolUsage075[0];
	}

	public double getOutPoolUsage075Avg() {
		return average(outPoolUsage075);
	}

	public double getOutPoolUsage095() {
		return outPoolUsage095[0];
	}

	public double getOutPoolUsage095Avg() {
		return average(outPoolUsage095);
	}

	public double getOutPoolUsage099() {
		return outPoolUsage099[0];
	}

	public double getOutPoolUsage099Avg() {
		return average(outPoolUsage099);
	}

	public double getOutPoolUsageStdDev() {
		return outPoolUsageStdDev[0];
	}

	public double getOutPoolUsageStdDevAvg() {
		return average(outPoolUsageStdDev);
	}

	public double getNumRecordsInPerSecond() {
		return numRecordsInPerSecond[0];
	}

	public double getNumRecordsInPerSecondAvg() {
		return average(numRecordsInPerSecond);
	}

	public double getNumRecordsInPerSecond(int i) {
		if (i >= 0 && i < numRecordsInPerSecond.length) {
			return numRecordsInPerSecond[i];
		}
		return 0.0;
	}

	public double getNumRecordsOutPerSecond() {
		return numRecordsOutPerSecond[0];
	}

	public double getNumRecordsOutPerSecondAvg() {
		return average(numRecordsOutPerSecond);
	}

	public double getNumRecordsOutPerSecond(int i) {
		if (i >= 0 && i < numRecordsOutPerSecond.length) {
			return numRecordsOutPerSecond[i];
		}
		return 0.0;
	}

	private int average(int[] values) {
		int sum = 0;
		int count = 0;
		for (int i = 0; i < values.length; i++) {
			if (values[i] > 0) {
				sum = sum + values[i];
				count++;
			}
		}
		if (count == 0) {
			return 0;
		}
		return (int) Math.ceil(sum / count);
	}

	private long average(long[] values) {
		long sum = 0;
		int count = 0;
		for (int i = 0; i < values.length; i++) {
			if (values[i] > 0) {
				sum = sum + values[i];
				count++;
			}
		}
		if (count == 0) {
			return 0;
		}
		return (long) Math.ceil(sum / count);
	}

	private double average(double[] values) {
		double sum = 0;
		int count = 0;
		for (int i = 0; i < values.length; i++) {
			if (values[i] > 0) {
				sum = sum + values[i];
				count++;
			}
		}
		if (count == 0) {
			return 0;
		}
		return Math.ceil(sum / count);
	}
}
