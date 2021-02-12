package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRideDistanceCalculator;

/**
 * UDF to list <driverID, passengersCount, distance, time, # of counts>
 */
public class TaxiRidePassengerDistanceTimeTokenizerMap implements MapFunction<TaxiRide, Tuple5<Long, Double, Double, Double, Long>> {

	@Override
	public Tuple5<Long, Double, Double, Double, Long> map(TaxiRide ride) {
		double distance = TaxiRideDistanceCalculator.distance(
			ride.startLat,
			ride.startLon,
			ride.endLat,
			ride.endLon,
			"K");
		long elapsedTimeMilliSec = ride.endTime.getMillis() - ride.startTime.getMillis();
		Double elapsedTimeMinutes = Double.valueOf(elapsedTimeMilliSec * 1000 * 60);

		return Tuple5.of(
			ride.driverId,
			Double.valueOf(ride.passengerCnt),
			distance,
			elapsedTimeMinutes,
			1L);
	}
}
