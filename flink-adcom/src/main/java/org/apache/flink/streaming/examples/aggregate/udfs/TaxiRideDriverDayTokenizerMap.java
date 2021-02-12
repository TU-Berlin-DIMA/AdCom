package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

public class TaxiRideDriverDayTokenizerMap implements MapFunction<TaxiRide, Tuple3<Integer, Long, Long>> {
	@Override
	public Tuple3<Integer, Long, Long> map(TaxiRide ride) {
		return Tuple3.of(ride.dayOfTheYear, ride.driverId, 1L);
	}
}
