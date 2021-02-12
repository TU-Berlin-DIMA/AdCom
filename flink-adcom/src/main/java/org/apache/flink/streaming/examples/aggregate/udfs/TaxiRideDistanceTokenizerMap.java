package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRideDistanceCalculator;

import java.util.Random;

public class TaxiRideDistanceTokenizerMap implements MapFunction<TaxiRide, Tuple2<Integer, Double>> {
	private final Random random;

	public TaxiRideDistanceTokenizerMap() {
		random = new Random();
	}

	@Override
	public Tuple2<Integer, Double> map(TaxiRide ride) {
		// create random keys from 0 to 10 in order to average the passengers of all taxi drivers
		int low = 0;
		int high = 10;
		Integer result = random.nextInt(high - low) + low;

		Double distance = TaxiRideDistanceCalculator.distance(
			ride.startLat,
			ride.startLon,
			ride.endLat,
			ride.endLon,
			"K");
		return Tuple2.of(result, distance);
	}
}
