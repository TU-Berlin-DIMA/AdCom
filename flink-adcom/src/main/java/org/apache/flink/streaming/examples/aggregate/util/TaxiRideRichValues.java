package org.apache.flink.streaming.examples.aggregate.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

public class TaxiRideRichValues implements Comparable<TaxiRideRichValues>, Serializable {

	private static final transient DateTimeFormatter timeFormatter =
		DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
	public long rideId;
	public boolean isStart;
	public DateTime startTime;
	public DateTime endTime;
	public int dayOfTheYear;
	public float startLon;
	public float startLat;
	public float endLon;
	public float endLat;
	public short passengerCnt;
	public long taxiId;
	public long driverId;
	public double euclideanDistance;
	public double elapsedTime;

	public TaxiRideRichValues() {
	}

	public TaxiRideRichValues(TaxiRide taxiRide) {
		if (taxiRide != null) {
			this.rideId = taxiRide.rideId;
			this.isStart = taxiRide.isStart;
			this.startTime = taxiRide.startTime;
			this.endTime = taxiRide.endTime;
			this.dayOfTheYear = taxiRide.dayOfTheYear;
			this.startLon = taxiRide.startLon;
			this.startLat = taxiRide.startLat;
			this.endLon = taxiRide.endLon;
			this.endLat = taxiRide.endLat;
			this.passengerCnt = taxiRide.passengerCnt;
			this.taxiId = taxiRide.taxiId;
			this.driverId = taxiRide.driverId;
			this.euclideanDistance = getEuclideanDistance();
			this.elapsedTime = getElapsedTime();
		} else {
			System.out.println("WARNING: TaxiRide is null");
		}
	}

	public double getEuclideanDistance() {
		return TaxiRideDistanceCalculator.distance(
			this.startLat,
			this.startLon,
			this.endLat,
			this.endLon,
			"K");
	}

	public double getElapsedTime() {
		// elapsed time taxi ride
		long elapsedTimeMilliSec = this.endTime.getMillis() - this.startTime.getMillis();
		Double elapsedTimeMinutes = Double.valueOf(elapsedTimeMilliSec * 1000 * 60);
		return elapsedTimeMinutes;
	}

	@Override
	public int compareTo(TaxiRideRichValues other) {
		if (other == null) {
			return 1;
		}
		int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
		if (compareTimes == 0) {
			if (this.isStart == other.isStart) {
				return 0;
			} else {
				if (this.isStart) {
					return -1;
				} else {
					return 1;
				}
			}
		} else {
			return compareTimes;
		}
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof TaxiRideRichValues &&
			this.rideId == ((TaxiRideRichValues) other).rideId;
	}

	@Override
	public int hashCode() {
		return (int) this.rideId;
	}

	public long getEventTime() {
		if (isStart) {
			return startTime.getMillis();
		} else {
			return endTime.getMillis();
		}
	}
}
