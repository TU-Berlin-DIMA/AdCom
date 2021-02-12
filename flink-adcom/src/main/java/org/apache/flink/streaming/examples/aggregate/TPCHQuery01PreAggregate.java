package org.apache.flink.streaming.examples.aggregate;

import io.airlift.tpch.LineItem;

import org.apache.flink.api.common.functions.PreAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.aggregate.udfs.*;
import org.apache.flink.streaming.examples.aggregate.util.GenericParameters;

import static org.apache.flink.streaming.examples.aggregate.util.CommonParameters.*;

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
public class TPCHQuery01PreAggregate {
	public static void main(String[] args) throws Exception {
		// @formatter:off
		GenericParameters genericParam = new GenericParameters(args);
		genericParam.printParameters();

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (genericParam.isDisableOperatorChaining()) {
			env.disableOperatorChaining();
		}
		String slotGroup01 = SLOT_GROUP_DEFAULT;
		String slotGroup02 = SLOT_GROUP_DEFAULT;
		if (genericParam.getSlotSplit() == 0) {
			slotGroup01 = SLOT_GROUP_DEFAULT;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (genericParam.getSlotSplit() == 1) {
			slotGroup01 = SLOT_GROUP_01;
			slotGroup02 = SLOT_GROUP_DEFAULT;
		} else if (genericParam.getSlotSplit() == 2) {
			slotGroup01 = SLOT_GROUP_01;
			slotGroup02 = SLOT_GROUP_02;
		}

		DataStream<LineItem> lineItems = null;
		if (genericParam.isParallelSource()) {
			lineItems = env.addSource(new LineItemSourceParallel()).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		} else {
			lineItems = env.addSource(new LineItemSource()).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);
		}

		DataStream<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> lineItemsMap = lineItems
			.map(new LineItemToTuple11Map()).name(OPERATOR_TOKENIZER).uid(OPERATOR_TOKENIZER).slotSharingGroup(slotGroup01);

		DataStream<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> lineItemsCombined = null;
		PreAggregateFunction<String,
			Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>,
			Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>,
			Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> lineItemPreAggUDF = new LineItemSumPreAgg();
		if (!genericParam.isEnableController() && genericParam.getPreAggregationProcessingTimer() == -1) {
			// no combiner
			lineItemsCombined = lineItemsMap;
		} else if (!genericParam.isEnableController() && genericParam.getPreAggregationProcessingTimer() > 0) {
			// static combiner based on timeout
			lineItemsCombined = lineItemsMap.combine(lineItemPreAggUDF, genericParam.getPreAggregationProcessingTimer()).name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		} else if (genericParam.isEnableController()) {
			// dynamic combiner with PI controller
			lineItemsCombined = lineItemsMap.adCombine(lineItemPreAggUDF, genericParam.getPreAggregationProcessingTimer()).name(OPERATOR_PRE_AGGREGATE).uid(OPERATOR_PRE_AGGREGATE).slotSharingGroup(slotGroup01);
		}

		DataStream<Tuple2<String, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>>> sumAndAvgLineItems = lineItemsCombined
			.keyBy(new LineItemFlagAndStatusKeySelector())
			.reduce(new SumAndAvgLineItemReducer()).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());

		DataStream<String> result = sumAndAvgLineItems.map(new Tuple11ToLineItemResult()).name(OPERATOR_FLAT_OUTPUT).uid(OPERATOR_FLAT_OUTPUT).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());

		if (genericParam.getOutput().equalsIgnoreCase(SINK_DATA_MQTT)) {
			result.addSink(new MqttDataSink(TOPIC_DATA_SINK, genericParam.getSinkHost(), genericParam.getSinkPort())).name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());
		} else if (genericParam.getOutput().equalsIgnoreCase(SINK_TEXT)) {
			result.print().name(OPERATOR_SINK).uid(OPERATOR_SINK).slotSharingGroup(slotGroup02).setParallelism(genericParam.getParallelisGroup02());
		} else {
			System.out.println("discarding output");
		}
		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TPCHQuery01PreAggregate.class.getSimpleName());
		// @formatter:on
	}
}
