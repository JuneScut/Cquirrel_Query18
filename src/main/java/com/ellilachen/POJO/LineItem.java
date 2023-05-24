package com.ellilachen.POJO;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;

/**
 * Project name: CquirrelDemo
 * Class name：LineItem
 * Description：TODO
 * Create time：2023/2/1 21:11
 * Creator：ellilachen
 */
public class LineItem implements SQLIterator {
    public Boolean update;
    public int orderKey;
    public int partkey;
    public int suppKey;
    public int lineNumber;
    public double quantity;

    public double extendedPrice;
    public double discount;
    public double tax;
    public String returnFlag;
    public String lineStatus;
    public Date shipDate;
    public Date commitDate;
    public Date receiptDate;
    public String shipinstruct;
    public String shipMode;
    public String comment;
    public String tag;

    public LineItem() {
    }

    public LineItem(Boolean update, int orderKey, int partkey, int suppKey, int lineNumber, double quantity, double extendedPrice, double discount, double tax, String returnFlag, String lineStatus, Date shipDate, Date commitDate, Date receiptDate, String shipinstruct, String shipMode, String comment, String tag) {
        this.update = update;
        this.orderKey = orderKey;
        this.partkey = partkey;
        this.suppKey = suppKey;
        this.lineNumber = lineNumber;
        this.quantity = quantity;
        this.extendedPrice = extendedPrice;
        this.discount = discount;
        this.tax = tax;
        this.returnFlag = returnFlag;
        this.lineStatus = lineStatus;
        this.shipDate = shipDate;
        this.commitDate = commitDate;
        this.receiptDate = receiptDate;
        this.shipinstruct = shipinstruct;
        this.shipMode = shipMode;
        this.comment = comment;
        this.tag = tag;
    }

    public LineItem(String[] fields) {
        if (fields.length < 17) {
            return;
        }
        this.update = fields[0].equals("+");
        this.orderKey = Integer.parseInt(fields[1]);
        this.partkey = Integer.parseInt(fields[2]);
        this.suppKey = Integer.parseInt(fields[3]);
        this.lineNumber = Integer.parseInt(fields[4]);
        this.quantity = Double.parseDouble(fields[5]);
        this.extendedPrice = Double.parseDouble(fields[6]);
        this.discount = Double.parseDouble(fields[7]);
        this.tax = Double.parseDouble(fields[8]);
        this.returnFlag = fields[9];
        this.lineStatus = fields[10];
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy",  Locale.US);// 26/7/1992
        try {
            this.shipDate = new Date(simpleDateFormat.parse(fields[11]).getTime());
            this.commitDate = new Date(simpleDateFormat.parse(fields[12]).getTime());
            this.receiptDate = new Date(simpleDateFormat.parse(fields[13]).getTime());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        this.shipinstruct = fields[14];
        this.shipMode = fields[15];
        this.comment = fields[16];
        this.tag = fields[17];
    }

    @Override
    public String[] getKeys() {
        return new String[] {"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT",
                "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT", "tag"};
    }

    @Override
    public Object[] getValues() {
        ArrayList<Object> values = new ArrayList<>();
        values.add(this.orderKey);
        values.add(this.partkey);
        values.add(this.suppKey);
        values.add(this.lineNumber);
        values.add(this.quantity);
        values.add(this.extendedPrice);
        values.add(this.discount);
        values.add(this.tax);
        values.add(this.returnFlag);
        values.add(this.lineStatus);
        values.add(this.shipDate);
        values.add(this.commitDate);
        values.add(this.receiptDate);
        values.add(this.shipinstruct);
        values.add(this.shipMode);
        values.add(this.comment);
        values.add(this.tag);
        return values.toArray();
    }

}
