package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

/**
 * UDF to list <driverID, passengersCount, # of counts>
 */
public class TaxiRidePassengerTokenizerMap implements MapFunction<TaxiRide, Tuple3<Long, Double, Long>> {

	@Override
	public Tuple3<Long, Double, Long> map(TaxiRide ride) {
		return Tuple3.of(ride.driverId, Double.valueOf(ride.passengerCnt), 1L);
	}
}
