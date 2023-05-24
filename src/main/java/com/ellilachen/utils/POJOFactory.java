package com.ellilachen.utils;

import com.ellilachen.POJO.Customer;
import com.ellilachen.POJO.LineItem;
import com.ellilachen.POJO.Order;
import com.ellilachen.POJO.SQLIterator;
import com.ellilachen.Tables;

import java.security.InvalidParameterException;

/**
 * Project name: CquirrelDemo
 * Class name：POJOFactory
 * Description：TODO
 * Create time：2023/2/5 23:43
 * Creator：ellilachen
 */
public class POJOFactory {
    static public SQLIterator of(String tag, String[] fields) {
        if (fields.length > 1) {
            if (tag.equals(Tables.customer.name())) {
                return new Customer(fields);
            } else if (tag.equals(Tables.orders.name())) {
                return new Order(fields);
            } else if (tag.equals(Tables.lineitem.name())) {
                return new LineItem(fields);
            } else {
                throw new InvalidParameterException("Do not support this kind of tag");
            }
        } else {
            throw new InvalidParameterException("There are no enough fields");
        }
    }
}
