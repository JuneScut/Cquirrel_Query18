package com.ellilachen.operators;

import com.ellilachen.utils.OutputBoolean;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Date;


/**
 * Project name: CquirrelDemo
 * Class name：ConnectTmpAndSub
 * Description：TODO
 * Create time：2023/2/11 15:35
 * Creator：ellilachen
 */
public class ConnectTmpAndSub extends CoProcessFunction<
        Tuple6<Boolean, String, Integer, Integer, Date, Double>,
        Tuple3<Boolean, Integer, Double>,
        Tuple7<Boolean, String, Integer, Integer, Date, Double, Double>> {

    private ListState<Tuple6<Boolean, String, Integer, Integer, Date, Double>> leftListState;
    private ListState<Tuple3<Boolean, Integer, Double>> rightListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple6<Boolean, String, Integer, Integer, Date, Double>>("leftList",
                        Types.TUPLE(Types.BOOLEAN, Types.STRING, Types.INT, Types.INT, Types.LOCAL_DATE, Types.DOUBLE))
        );

        rightListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple3<Boolean, Integer, Double>>("rightList",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.DOUBLE))
        );
    }

    @Override
    public void processElement1(Tuple6<Boolean, String, Integer, Integer, Date, Double> left, CoProcessFunction<Tuple6<Boolean, String, Integer, Integer, Date, Double>, Tuple3<Boolean, Integer, Double>, Tuple7<Boolean, String, Integer, Integer, Date, Double, Double>>.Context context, Collector<Tuple7<Boolean, String, Integer, Integer, Date, Double, Double>> collector) throws Exception {
        leftListState.add(left);
        for (Tuple3<Boolean, Integer, Double> right : rightListState.get()) {
            boolean update = OutputBoolean.isUpdate(left.f0, right.f0);
            if (OutputBoolean.canCollect(left.f0, right.f0)) {
                Tuple7<Boolean, String, Integer, Integer, Date, Double, Double> tuple = Tuple7.of(update, left.f1, left.f2, left.f3, left.f4, left.f5, right.f2);
                collector.collect(tuple);
            }
        }
    }

    @Override
    public void processElement2(Tuple3<Boolean, Integer, Double> right, CoProcessFunction<Tuple6<Boolean, String, Integer, Integer, Date, Double>, Tuple3<Boolean, Integer, Double>, Tuple7<Boolean, String, Integer, Integer, Date, Double, Double>>.Context context, Collector<Tuple7<Boolean, String, Integer, Integer, Date, Double, Double>> collector) throws Exception {
        rightListState.add(right);
        for (Tuple6<Boolean, String, Integer, Integer, Date, Double> left : leftListState.get()) {
            boolean update = OutputBoolean.isUpdate(left.f0, right.f0);
            if (OutputBoolean.canCollect(left.f0, right.f0)) {
                Tuple7<Boolean, String, Integer, Integer, Date, Double, Double> tuple = Tuple7.of(update, left.f1, left.f2, left.f3, left.f4, left.f5, right.f2);
                collector.collect(tuple);
            }
        }
    }
}
