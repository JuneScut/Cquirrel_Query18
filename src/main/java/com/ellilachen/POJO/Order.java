package com.ellilachen.POJO;

import java.beans.Introspector;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;

/**
 * Project name: CquirrelDemo
 * Class name：Order
 * Description：TODO
 * Create time：2023/2/1 21:11
 * Creator：ellilachen
 */
public class Order implements SQLIterator {
    public Boolean update;
    public int orderKey;
    public int custKey;
    public String orderStatus;
    public double totalPrice;
    public Date orderDate;
    public String orderPriority;
    public String clerk;
    public String shipPriority;
    public String comment;

    public String tag;

    public Order() {
    }

    public Order(Boolean update, int orderKey, int custKey, String orderStatus, double totalPrice, Date orderDate, String orderPriority, String clerk, String shipPriority, String comment, String tag) {
        this.update = update;
        this.orderKey = orderKey;
        this.custKey = custKey;
        this.orderStatus = orderStatus;
        this.totalPrice = totalPrice;
        this.orderDate = orderDate;
        this.orderPriority = orderPriority;
        this.clerk = clerk;
        this.shipPriority = shipPriority;
        this.comment = comment;
        this.tag = tag;
    }

    public Order(String[] fields) {
        if (fields.length < 11) {
            return;
        }
        this.update = fields[0].equals("+");
        this.orderKey = Integer.parseInt(fields[1]);
        this.custKey = Integer.parseInt(fields[2]);
        this.orderStatus = fields[3];
        this.totalPrice = Float.parseFloat(fields[4]);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy",  Locale.US);// 26/7/1992
        try {
            this.orderDate = new Date(simpleDateFormat.parse(fields[5]).getTime());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        this.orderPriority = fields[6];
        this.clerk = fields[7];
        this.shipPriority = fields[8];
        this.comment = fields[9];
        this.tag = fields[10];
    }

    @Override
    public String[] getKeys() {
        return new String[]{
                "O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE",
                "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT", "tag"
        };
    }

    @Override
    public Object[] getValues() {
        ArrayList<Object> values = new ArrayList<Object>();
        values.add(orderKey);
        values.add(custKey);
        values.add(orderStatus);
        values.add(totalPrice);
        values.add(orderDate);
        values.add(orderPriority);
        values.add(clerk);
        values.add(shipPriority);
        values.add(comment);
        values.add(tag);
        return values.toArray();
    }
}
