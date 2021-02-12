package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

public class TaxiRideDriverPassengerTokenizerMap implements MapFunction<TaxiRide, Tuple2<Long, Long>> {
	@Override
	public Tuple2<Long, Long> map(TaxiRide ride) {
		return new Tuple2<Long, Long>(ride.driverId, Long.valueOf(ride.passengerCnt));
	}
}
