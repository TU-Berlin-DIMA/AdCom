package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;

/**
 * A dummy function just to increase the parallelism of the Local Aggregation at Table API
 */
public class TaxiRideDummyMap implements MapFunction<TaxiRide, TaxiRide> {
	@Override
	public TaxiRide map(TaxiRide value) throws Exception {
		return value;
	}
}
