package com.ellilachen.operators;

import com.ellilachen.POJO.Customer;
import com.ellilachen.POJO.Order;
import com.ellilachen.utils.OutputBoolean;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Date;


/**
 * Project name: CquirrelDemo
 * Class name：ConnectCustAndOrder
 * Description：一对多 join
 * Create time：2023/2/11 14:52
 * Creator：ellilachen
 */

public class ConnectCusAndOrder extends CoProcessFunction<Customer, Order, Tuple6<Boolean, String, Integer, Integer, Date, Double>> {
    private ValueState<Customer> customerState;

    private ListState<Order> orderListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        customerState = getRuntimeContext().getState(
                new ValueStateDescriptor<Customer>("customerState", Customer.class)
        );
        orderListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Order>("orderState", Order.class)
        );
    }

    @Override
    public void processElement1(Customer customer, CoProcessFunction<Customer, Order, Tuple6<Boolean, String, Integer, Integer, Date, Double>>.Context context, Collector<Tuple6<Boolean, String, Integer, Integer, Date, Double>> collector) throws Exception {
        Customer oldCustomer = customerState.value();

        if ((oldCustomer == null && customer.update) || (oldCustomer != null && !customer.update)) {
            customerState.update(customer);
            for (Order order: orderListState.get()) {
                boolean update = OutputBoolean.isUpdate(customer.update, order.update);
                Tuple6<Boolean, String, Integer, Integer, Date, Double> tuple = Tuple6.of(update, customer.name, customer.custKey, order.orderKey, order.orderDate, order.totalPrice);
                if (OutputBoolean.canCollect(customer.update, order.update)) {
                    collector.collect(tuple);
                }
            }
        }
    }

    @Override
    public void processElement2(Order order, CoProcessFunction<Customer, Order, Tuple6<Boolean, String, Integer, Integer, Date, Double>>.Context context, Collector<Tuple6<Boolean, String, Integer, Integer, Date, Double>> collector) throws Exception {
        boolean canAddToList = order.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (Order o : orderListState.get()) {
                if (o.update) {
                    insertCount += 1;
                } else {
                    deleteCount += 1;
                }
            }
            if (insertCount > deleteCount) {
                canAddToList = true;
            }
        }
        if (!canAddToList) {
            return;
        }
        orderListState.add(order);
        Customer customer = customerState.value();
        if (customer != null) {
            boolean update = OutputBoolean.isUpdate(customer.update, order.update);
            Tuple6<Boolean, String, Integer, Integer, Date, Double> tuple = Tuple6.of(update, customer.name, customer.custKey, order.orderKey, order.orderDate, order.totalPrice);
            if (OutputBoolean.canCollect(customer.update, order.update)) {
                collector.collect(tuple);
            }
        }
    }
}
