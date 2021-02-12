package org.apache.flink.streaming.examples.aggregate.udfs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRideRichValues;


public class TaxiRideRichValuesMap implements MapFunction<TaxiRide, TaxiRideRichValues> {
	@Override
	public TaxiRideRichValues map(TaxiRide value) throws Exception {
		return new TaxiRideRichValues(value);
	}
}
