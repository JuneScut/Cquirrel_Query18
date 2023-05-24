package com.ellilachen.operators;

import com.ellilachen.POJO.LineItem;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Project name: CquirrelDemo
 * Class name：LineItemSumFunc
 * Description：TODO
 * Create time：2023/2/11 19:03
 * Creator：ellilachen
 */
public class LineItemSubProcess extends KeyedProcessFunction<Integer, LineItem, Tuple3<Boolean, Integer, Double>> {
    private transient ValueState<Double> sumState;
    private int bound;

    public LineItemSubProcess(int bound) {
        this.bound = bound;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sumState = getRuntimeContext().getState(
                new ValueStateDescriptor<Double>("sumState", Types.DOUBLE)
        );
    }

    @Override
    public void processElement(LineItem lineItem, KeyedProcessFunction<Integer, LineItem, Tuple3<Boolean, Integer, Double>>.Context context, Collector<Tuple3<Boolean, Integer, Double>> collector) throws Exception {
        Double prevSum = sumState.value();
        double sum = prevSum != null ? prevSum : 0;

        if (lineItem.update) {
            sum += lineItem.quantity;
            sumState.update(sum);
            if (sum > bound) {
                collector.collect(Tuple3.of(lineItem.update, lineItem.orderKey, sum));
            }
        } else {
            if (sum > bound) {
                collector.collect(Tuple3.of(false, lineItem.orderKey, sum));
            }
            sum -= lineItem.quantity;
            sumState.update(sum);
            if (sum > bound) {
                collector.collect(Tuple3.of(true, lineItem.orderKey, sum));
            }
        }
    }
}
