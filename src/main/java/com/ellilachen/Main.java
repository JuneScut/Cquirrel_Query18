package com.ellilachen;

import com.ellilachen.POJO.Customer;
import com.ellilachen.POJO.LineItem;
import com.ellilachen.POJO.Order;
import com.ellilachen.operators.ConnectCusAndOrder;
import com.ellilachen.operators.ConnectTmpAndSub;
import com.ellilachen.operators.DataSource;
import com.ellilachen.operators.LineItemSubProcess;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Main {
    private static final OutputTag<LineItem> lineItemOutputTag = new OutputTag<LineItem>("lineitem"){};
    private static final OutputTag<Order> ordersOutputTag = new OutputTag<Order>("orders"){};
    private static final OutputTag<Customer> customerOutputTag = new OutputTag<Customer>("customer"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.addSource(new DataSource()).setParallelism(1);
        SingleOutputStreamOperator<String> splitStream = stream.process(new SpiltStreamFunc());

        DataStream<Customer> customerDataStream = splitStream.getSideOutput(customerOutputTag);
        DataStream<Order> orderDataStream = splitStream.getSideOutput(ordersOutputTag);
        DataStream<LineItem> lineItemDataStream = splitStream.getSideOutput(lineItemOutputTag);

        env.setParallelism(8);

        SingleOutputStreamOperator<Tuple3<Boolean, Integer, Double>> subOrderStream = lineItemDataStream
                .keyBy(lineItem -> lineItem.orderKey)
                .process(new LineItemSubProcess(300));

        customerDataStream
                .connect(orderDataStream)
                .keyBy(c -> c.custKey, o -> o.custKey)
                .process(new ConnectCusAndOrder())
                .connect(subOrderStream)
                .keyBy(t1 -> t1.f3, t2 -> t2.f1)
                .process(new ConnectTmpAndSub())
                .print();

        env.execute();
    }

    public static class SpiltStreamFunc extends ProcessFunction<String, String> {
        @Override
        public void processElement(String line, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
            String[] fields = line.split("\t");
            String tag = fields[fields.length-1];

            switch (tag) {
                case "customer":
                    context.output(customerOutputTag, new Customer(fields));
                    break;
                case "orders":
                    context.output(ordersOutputTag, new Order(fields));
                    break;
                case "lineitem":
                    context.output(lineItemOutputTag, new LineItem(fields));
                    break;
                default:
                    break;
            }

        }
    }

}