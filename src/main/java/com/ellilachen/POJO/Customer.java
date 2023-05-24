package com.ellilachen.POJO;

import java.util.ArrayList;

/**
 * Project name: CquirrelDemo
 * Class name：Customer
 * Description：TODO
 * Create time：2023/2/1 21:11
 * Creator：ellilachen
 */
public class Customer implements SQLIterator{
    public Boolean update;
    public int custKey;
    public String name;
    public String address;
    public String nationKey;
    public String phone;
    public float acctbal;
    public String mktsegment;
    public String comment;

    public String tag;

    public Customer() {}

    public Customer(Boolean update, int custKey, String name, String nationKey, String phone, float acctbal, String mktsegment, String comment, String tag) {
        this.update = update;
        this.custKey = custKey;
        this.name = name;
        this.nationKey = nationKey;
        this.phone = phone;
        this.acctbal = acctbal;
        this.mktsegment = mktsegment;
        this.comment = comment;
        this.tag = tag;
    }

    public Customer(String[] fields) {
        if (fields.length < 10) {
            return;
        }
        this.update = fields[0].equals("+");
        this.custKey = Integer.parseInt(fields[1]);
        this.name = fields[2];
        this.address = fields[3];
        this.nationKey = fields[4];
        this.phone = fields[5];
        this.acctbal = Float.parseFloat(fields[6]);
        this.mktsegment = fields[7];
        this.comment = fields[8];
        this.tag = fields[9];
    }

    @Override
    public String[] getKeys() {
        return new String[] {"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT", "tag"};
    }


    @Override
    public Object[] getValues() {
        ArrayList<Object> values = new ArrayList<>();
        values.add(this.custKey);
        values.add(this.name);
        values.add(this.address);
        values.add(this.nationKey);
        values.add(this.phone);
        values.add(this.acctbal);
        values.add(this.mktsegment);
        values.add(this.comment);
        values.add(this.tag);
        return values.toArray();
    }

}
