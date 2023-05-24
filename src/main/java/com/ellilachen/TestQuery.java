package com.ellilachen;

import com.ellilachen.POJO.SQLIterator;
import com.ellilachen.utils.JDBCUtil;
import com.ellilachen.utils.POJOFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Project name: CquirrelDemo
 * Class name：TestResult
 * Description：TODO
 * Create time：2023/2/5 21:48
 * Creator：ellilachen
 */
public class TestQuery {
    public static void readTable() {
        String querySQL = "select\n" +
                "    c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)\n" +
                "from\n" +
                "    customer, orders, lineitem\n" +
                "where\n" +
                "    o_orderkey in ( \n" +
                "        select\n" +
                "            l_orderkey\n" +
                "        from\n" +
                "            lineitem\n" +
                "        group by \n" +
                "            l_orderkey having\n" +
                "            sum(l_quantity) > 270\n" +
                "    )\n" +
                "    and c_custkey = o_custkey\n" +
                "    and o_orderkey = l_orderkey \n" +
                "group by\n" +
                "    c_name,\n" +
                "    c_custkey,\n" +
                "    o_orderkey,\n" +
                "    o_orderdate,\n" +
                "    o_totalprice;";

        ResultSet rs = JDBCUtil.executeSQL(querySQL);
        try {
            while (rs.next()) {
//                int orderKey = rs.getInt(1); // 注意：索引从1开始
//                double revenue = rs.getDouble(2);
//                Date orderDate = rs.getDate(3);
//                String priority = rs.getString(4);
//                System.out.println("(" + orderKey + ", " + revenue + ", " + orderDate + " ," + priority + ")");

                String name = rs.getString(1);
                int custKey = rs.getInt(2);
                int orderKey = rs.getInt(3);
                Date orderDate = rs.getDate(4);
                double totalPrice = rs.getDouble(5);
                double quantity = rs.getDouble(6);

                System.out.printf("(%s, %s, %s, %s, %2f, %2f)\n", name, custKey, orderKey, orderDate, totalPrice, quantity);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JDBCUtil.truncateTable("customer");
        JDBCUtil.truncateTable("orders");
        JDBCUtil.truncateTable("lineItem");

        Path path = Path.of("data/input_sf_0_5.txt");
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        int epoch = 1;

        for (String line : lines) {
            if (!line.equals("Bye")) {
                String[] fields = line.split("\t");
                int length = fields.length;
                String operator = fields[0];
                String tag = fields[length - 1];
                SQLIterator row = POJOFactory.of(tag, fields);
                if (operator.equals("+")) {
                    JDBCUtil.insertIntoTable(tag, row.getKeys(), row.getValues());
                } else if (operator.equals("-")) {
                    JDBCUtil.deleteFromTable(tag, Arrays.copyOfRange(row.getKeys(), 0, 2), Arrays.copyOfRange(row.getValues(), 0, 2));
                }
            } else {
                System.out.println("====================");
                readTable();
            }
        }
        JDBCUtil.close();
    }
}
