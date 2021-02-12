package org.apache.flink.streaming.examples.aggregate;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.udfs.*;
import org.apache.flink.streaming.examples.aggregate.util.GenericParameters;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRide;
import org.apache.flink.streaming.examples.aggregate.util.TaxiRideRichValues;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.logging.log4j.util.Strings;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * <pre>
 * -disableOperatorChaining false -input-par true -output mqtt -sinkHost 127.0.0.1 -mini_batch_enabled true -mini_batch_latency 1_s -mini_batch_size 1000 -mini_batch_two_phase true -parallelism-table 4
 * </pre>
 */
public class TaxiRideAvgTablePreAggregate {
	public static void main(String[] args) throws Exception {
		// @formatter:off
		GenericParameters genericParam = new GenericParameters(args);
		genericParam.printParameters();

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// access flink configuration
		Configuration configuration = tableEnv.getConfig().getConfiguration();
		// set low-level key-value options
		configuration.setInteger("table.exec.resource.default-parallelism", genericParam.getParallelismTableApi());
		// local-global aggregation depends on mini-batch is enabled
		configuration.setString("table.exec.mini-batch.enabled", Boolean.toString(genericParam.isMini_batch_enabled()));
		if (!Strings.isEmpty(genericParam.getMini_batch_allow_latency())) {
			configuration.setString("table.exec.mini-batch.allow-latency", genericParam.getMini_batch_allow_latency());
		}
		if (genericParam.getMini_batch_size() > 0) {
			configuration.setString("table.exec.mini-batch.size", String.valueOf(genericParam.getMini_batch_size()));
		}
		// enable two-phase, i.e. local-global aggregation
		if (genericParam.isTwoPhaseAgg()) {
			configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
		} else {
			configuration.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE");
		}
		if (genericParam.isDisableOperatorChaining()) {
			env.disableOperatorChaining();
		}

		DataStream<TaxiRide> rides = null;
		if (genericParam.isParallelSource()) {
			rides = env.addSource(new TaxiRideSourceParallel()).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE);
		} else {
			rides = env.addSource(new TaxiRideSource()).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE);
		}

		DataStream<TaxiRideRichValues> ridesToken = rides.map(new TaxiRideRichValuesMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).disableChaining();

		// "rideId, isStart, startTime, endTime, startLon, startLat, endLon, endLat, passengerCnt, taxiId, driverId, euclideanDistance, elapsedTime"
		Table ridesTableStream = tableEnv.fromDataStream(ridesToken);

		Table resultTableStream = ridesTableStream
			.groupBy($("driverId"))
			.select($("driverId"),
				$("passengerCnt").avg().as("passengerAvg"),
				$("euclideanDistance").avg().as("euclideanDistanceAvg"),
				$("elapsedTime").avg().as("elapsedTimeAvg")
			);

		TypeInformation<Tuple4<Long, Long, Double, Double>> typeInfo = TypeInformation.of(new TypeHint<Tuple4<Long, Long, Double, Double>>() {
		});
		DataStream<String> rideCounts = tableEnv
			.toRetractStream(resultTableStream, typeInfo)
			.map(new TaxiRideAvgTableOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT);

		if (genericParam.getOutput().equalsIgnoreCase(SINK_DATA_MQTT)) {
			rideCounts.addSink(new MqttDataSink(TOPIC_DATA_SINK, genericParam.getSinkHost(), genericParam.getSinkPort())).name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else if (genericParam.getOutput().equalsIgnoreCase(SINK_TEXT)) {
			rideCounts.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else {
			System.out.println("discarding output");
		}

		System.out.println(env.getExecutionPlan());
		env.execute(TaxiRideAvgTablePreAggregate.class.getSimpleName());
		// @formatter:on
	}
}
