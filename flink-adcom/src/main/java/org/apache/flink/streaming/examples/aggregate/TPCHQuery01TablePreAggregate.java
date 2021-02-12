package org.apache.flink.streaming.examples.aggregate;

import io.airlift.tpch.LineItem;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.udfs.*;
import org.apache.flink.streaming.examples.aggregate.util.GenericParameters;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.logging.log4j.util.Strings;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * 1 Q1 - Pricing Summary Report Query
 * https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Sample%20querys.20.xml
 *
 * <pre>
 * select
 *        l_returnflag,
 *        l_linestatus,
 *        sum(l_quantity) as sum_qty,
 *        sum(l_extendedprice) as sum_base_price,
 *        sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
 *        sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
 *        avg(l_quantity) as avg_qty,
 *        avg(l_extendedprice) as avg_price,
 *        avg(l_discount) as avg_disc,
 *        count(*) as count_order
 *  from
 *        lineitem
 *  where
 *        l_shipdate <= mdy (12, 01, 1998 ) - 90 units day
 *  group by
 *        l_returnflag,
 *        l_linestatus
 *  order by
 *        l_returnflag,
 *        l_linestatus;
 * </pre>
 * <p>
 * /bin/flink run TPCHQuery01PreAggregate.jar -pre-aggregate-window 1 -output mqtt -sinkHost 130.239.48.135
 * /bin/flink run TPCHQuery01PreAggregate.jar -pre-aggregate-window 1 -output mqtt -sinkHost 130.239.48.135 -slotSplit 1 -parallelism-group-01 16 -parallelism-group-02 16
 * /bin/flink run TPCHQuery01PreAggregate.jar -pre-aggregate-window 1 -output mqtt -sinkHost 130.239.48.135 -slotSplit 1 -parallelism-group-02 16
 * /bin/flink run TPCHQuery01PreAggregate.jar -pre-aggregate-window 1 -output mqtt -sinkHost 130.239.48.135 -slotSplit 1 -parallelism-group-02 24
 */
public class TPCHQuery01TablePreAggregate {
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

		DataStream<LineItem> lineItems = null;
		if (genericParam.isParallelSource()) {
			lineItems = env.addSource(new LineItemSourceParallel()).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE);
		} else {
			lineItems = env.addSource(new LineItemSource()).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE);
		}
		DataStream<Tuple12<String, String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> lineItemsMap = lineItems
			.map(new LineItemToTupleTableMap()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER);

		// root
		// |-- f0: STRING key
		// |-- f1: STRING ReturnFlag
		// |-- f2: STRING Status
		// |-- f3: BIGINT Quantity
		// |-- f4: DOUBLE ExtendedPrice
		// |-- f5: DOUBLE Discount
		// |-- f6: DOUBLE ExtendedPrice * (1 - Discount)
		// |-- f7: DOUBLE ExtendedPrice * (1 - Discount()) * (1 + Tax))
		// |-- f8: BIGINT Quantity
		// |-- f9: DOUBLE ExtendedPrice
		// |-- f10: DOUBLE Discount
		// |-- f11: BIGINT CountOrder
		Table lineItemsTableStream = tableEnv.fromDataStream(lineItemsMap);

		Table resultTableStream = lineItemsTableStream
			.groupBy($("f1"), $("f2"))
			.select(
				$("f1").as("returnflag"),
				$("f2").as("linestatus"),
				$("f3").sum().as("sum_qty"),
				$("f4").sum().as("sum_base_price"),
				$("f6").sum().as("sum_disc_price"),
				$("f7").sum().as("sum_charge"),
				$("f3").avg().as("avg_qty"),
				$("f4").avg().as("avg_price"),
				$("f5").avg().as("avg_disc"),
				$("f11").count().as("count_order")
			);
		resultTableStream.printSchema();

		TypeInformation<Tuple10<String, String, Long, Double, Double, Double, Long, Double, Double, Long>> typeInfo = TypeInformation.of(new TypeHint<Tuple10<String, String, Long, Double, Double, Double, Long, Double, Double, Long>>() {
		});
		DataStream<String> lineItemCounts = tableEnv
			.toRetractStream(resultTableStream, typeInfo)
			.map(new LineItemTableOutputMap()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT);

		if (genericParam.getOutput().equalsIgnoreCase(SINK_DATA_MQTT)) {
			lineItemCounts.addSink(new MqttDataSink(TOPIC_DATA_SINK, genericParam.getSinkHost(), genericParam.getSinkPort())).name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else if (genericParam.getOutput().equalsIgnoreCase(SINK_TEXT)) {
			lineItemCounts.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else {
			System.out.println("discarding output");
		}

		System.out.println(env.getExecutionPlan());
		env.execute(TPCHQuery01TablePreAggregate.class.getSimpleName());
	}
}
