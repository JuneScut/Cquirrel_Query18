package com.ellilachen;

import com.ellilachen.POJO.Customer;
import com.ellilachen.POJO.LineItem;
import com.ellilachen.POJO.Order;
import com.ellilachen.operators.DataSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Project name: ${PROJECT_NAME}
 * Class name：${NAME}
 * Description：TODO
 * Create time：${DATE} ${TIME}
 * Creator：${USER}
 */

public class Demo {
    private static final OutputTag<LineItem> lineItemOutputTag = new OutputTag<LineItem>("lineItem"){};
    private static final OutputTag<Order> ordersOutputTag = new OutputTag<Order>("orders"){};
    private static final OutputTag<Customer> customerOutputTag = new OutputTag<Customer>("customer"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.addSource(new DataSource()).setParallelism(1);
        SingleOutputStreamOperator<String> processStream = stream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                String[] fields = s.split("\t");
                int lastIndex = fields.length - 1;
                String tag = fields[lastIndex];
                if (tag.equals(Tables.lineitem.name())) {
                    LineItem lineItem = new LineItem(fields);
                    context.output(lineItemOutputTag, lineItem);
                } else if (tag.equals(Tables.orders.name())) {
                    Order order = new Order(fields);
                    context.output(ordersOutputTag, order);
                } else if (tag.equals(Tables.customer.name())) {
                    Customer customer = new Customer(fields);
                    context.output(customerOutputTag, customer);
                }
            }
        });

        DataStream<Customer> customerDataStream = processStream.getSideOutput(customerOutputTag);
        DataStream<Order> orderDataStream = processStream.getSideOutput(ordersOutputTag);
        DataStream<LineItem> lineItemDataStream = processStream.getSideOutput(lineItemOutputTag);

        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy",  Locale.US);// 26/7/1992

        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("data/output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(30))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(15))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        customerDataStream
                .connect(orderDataStream)
                .keyBy((c) -> c.custKey, (o) -> o.custKey)
                .process(new ConnectCusAndOrder())
                .connect(lineItemDataStream)
                .keyBy((t) -> t.f2, (l) -> l.orderKey)
                .process(new ConnectTempAndLineItem())
                .keyBy((t) -> t.f2 + simpleDateFormat.format(t.f4) + t.f5)
                .flatMap(new AggFunction())
                .print();

        env.execute();
    }

    public static class ConnectCusAndOrder extends CoProcessFunction<Customer, Order, Tuple5<Boolean, Integer, Integer, Date, String>> {
        // 一对多 join
        private ValueState<Customer> customerState;
        private ListState<Order> orderListState;


        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy",  Locale.US);// 26/7/1992
        private final Date endDate = simpleDateFormat.parse("31/03/1995");
        private final String segment = "AUTOMOBILE";

        public ConnectCusAndOrder() throws ParseException {
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            customerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Customer>("customer", Types.POJO(Customer.class)));
            orderListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Order>("order", Types.POJO(Order.class)));
        }

        @Override
        public void processElement1(Customer customer, CoProcessFunction<Customer, Order, Tuple5<Boolean, Integer, Integer, Date, String>>.Context context, Collector<Tuple5<Boolean, Integer, Integer, Date, String>> collector) throws Exception {
            customerState.update(customer);
            for (Order order: orderListState.get()) {
                Boolean update = customer.update && order.update;
                Tuple5<Boolean, Integer, Integer, Date, String> tuple = Tuple5.of(update, customer.custKey, order.orderKey, order.orderDate, order.shipPriority);
                if (update || (customer.update ^ order.update)) {
                    collector.collect(tuple);
                }
            }
        }
        @Override
        public void processElement2(Order order, CoProcessFunction<Customer, Order, Tuple5<Boolean, Integer, Integer, Date, String>>.Context context, Collector<Tuple5<Boolean, Integer, Integer, Date, String>> collector) throws Exception {
            orderListState.add(order);
            Customer customer = customerState.value();
            if (customer != null) {
                Boolean update = order.update && customer.update;
                Tuple5<Boolean, Integer, Integer, Date, String> tuple = Tuple5.of(update, customer.custKey, order.orderKey, order.orderDate, order.shipPriority);
                if (update || (customer.update ^ order.update)) {
                    collector.collect(tuple);
                }
            }
        }

    }

    public static class ConnectTempAndLineItem extends CoProcessFunction<Tuple5<Boolean, Integer, Integer, Date, String>, LineItem, Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>> {
        private ValueState<Tuple5<Boolean, Integer, Integer, Date, String>> cusOrderState;
        private ListState<LineItem> lineItemListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            cusOrderState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple5<Boolean, Integer, Integer, Date, String>>("cusOrder", Types.TUPLE(Types.BOOLEAN, Types.INT, Types.INT))
            );
            lineItemListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LineItem>("lineItem", LineItem.class)
            );
        }

        @Override
        public void processElement1(Tuple5<Boolean, Integer, Integer, Date, String> cusOrder, CoProcessFunction<Tuple5<Boolean, Integer, Integer, Date, String>, LineItem, Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>>.Context context, Collector<Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>> collector) throws Exception {
            cusOrderState.update(cusOrder);
            for (LineItem lineItem: lineItemListState.get()) {
                Boolean update = cusOrder.f0 && lineItem.update;
                Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double> tuple = Tuple8.of(update, cusOrder.f1, cusOrder.f2, lineItem.partkey, cusOrder.f3, cusOrder.f4, lineItem.extendedPrice, lineItem.discount);
                if (update || (cusOrder.f0 ^ lineItem.update)) {
                    collector.collect(tuple);
                }
            }
        }

        @Override
        public void processElement2(LineItem lineItem, CoProcessFunction<Tuple5<Boolean, Integer, Integer, Date, String>, LineItem, Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>>.Context context, Collector<Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>> collector) throws Exception {
            lineItemListState.add(lineItem);
            Tuple5<Boolean, Integer, Integer, Date, String> cusOrder = cusOrderState.value();
            if (cusOrder != null) {
                Boolean update = cusOrder.f0 && lineItem.update;
                Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double> tuple = Tuple8.of(update, cusOrder.f1, cusOrder.f2, lineItem.partkey, cusOrder.f3, cusOrder.f4, lineItem.extendedPrice, lineItem.discount);
                if (update || (cusOrder.f0 ^ lineItem.update)) {
                    collector.collect(tuple);
                }
            }
        }
    }

    public static class AggFunction extends RichFlatMapFunction<Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>, String> {
        private AggregatingState<Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>, Double> revenueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            revenueState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>, Double, Double>(
                            "sum",
                            new AggregateFunction<Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double>, Double, Double>() {
                                @Override
                                public Double createAccumulator() {
                                    return 0.0;
                                }
                                @Override
                                public Double add(Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double> tuple, Double acc) {
                                    if (tuple.f0) {
                                        return acc + tuple.f6 * (1-tuple.f7);
                                    } else {
                                        return  acc;
                                    }
                                }
                                @Override
                                public Double getResult(Double result) {
                                    return result;
                                }
                                @Override
                                public Double merge(Double d1, Double d2) {
                                    return null;
                                }
                            },
                            Types.DOUBLE
                    )
            );
        }

        @Override
        public void flatMap(Tuple8<Boolean, Integer, Integer, Integer, Date, String, Double, Double> tuple, Collector<String> collector) throws Exception {
            revenueState.add(tuple);
            Tuple7<Boolean, Integer, Integer, Integer, Double, Date, String> innerResult = Tuple7.of(tuple.f0, tuple.f1, tuple.f2, tuple.f3, revenueState.get(), tuple.f4, tuple.f5);
            collector.collect(innerResult.toString());
        }
    }
}